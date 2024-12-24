package main

// This assumes a PMTiles spatial database described here:
// https://millsfield.sfomuseum.org/blog/2022/12/19/pmtiles-pip/

/*

./bin/reverse-geocode \
    -workers 5 \
    -emitter-uri csv:///usr/local/data/4sq/4sq.csv.bz2 \
    -spatial-database-uri 'pmtiles://?tiles=file:///usr/local/data/pmtiles/&database=whosonfirst-point-in-polygon-z13-20240406&enable-cache=true&pmtiles-cache-size=4096&zoom=13&layer=whosonfirst'

*/

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/whosonfirst/go-reader-database-sql"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"

	"github.com/dgraph-io/ristretto/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/sfomuseum/go-csvdict/v2"
	"github.com/whosonfirst/go-foursquare-places"
	"github.com/whosonfirst/go-foursquare-places/emitter"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	wof_reader "github.com/whosonfirst/go-whosonfirst-reader"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	hierarchy_filter "github.com/whosonfirst/go-whosonfirst-spatial/hierarchy/filter"
)

func main() {

	var spatial_database_uri string
	var properties_reader_uri string

	var emitter_uri string
	var workers int
	var start_after int64

	var profile bool
	var mem_profile string
	var cpu_profile string

	var verbose bool

	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "", "A registered whosonfirst/go-whosonfirst-spatial/database/SpatialDatabase URI to use for perforning reverse geocoding tasks.")

	flag.StringVar(&properties_reader_uri, "properties-reader-uri", "{spatial-database-uri}", "...")

	flag.StringVar(&emitter_uri, "emitter-uri", "", "A registered whosonfirst/go-foursquare-places/emitter.Emitter URI.")
	flag.IntVar(&workers, "workers", 5, "The maximum number of workers to process reverse geocoding tasks.")

	flag.Int64Var(&start_after, "start-after", 0, "If > 0 then delay processing for 'start_after' number of records.")

	flag.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging.")
	flag.BoolVar(&profile, "profile", false, "Enable pprof profiling.")

	flag.StringVar(&mem_profile, "mem-profile", "memprofile.pb.gz", "...")
	flag.StringVar(&cpu_profile, "cpu-profile", "cpuprofile.pb.gz", "...")

	flag.Parse()

	if profile {

		cpu_f, _ := os.Create(cpu_profile)
		defer cpu_f.Close()

		err := pprof.StartCPUProfile(cpu_f)

		if err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}

		defer func() {

			pprof.StopCPUProfile()

			f, _ := os.Create(mem_profile)
			defer f.Close()
			runtime.GC()
			pprof.WriteHeapProfile(f)

		}()
	}

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	// START OF json wah-wah...

	// https://pkg.go.dev/github.com/paulmach/orb/geojson#pkg-variables
	// https://github.com/json-iterator/go
	//
	// "Even the most widely used json-iterator will severely degrade in generic (no-schema) or big-volume JSON serialization and deserialization."
	// https://github.com/bytedance/sonic/blob/main/INTRODUCTION.md
	//
	// I have not verified that claim either way but since we're not trafficing in "big-volume" JSON files
	// I am just going to see how this (json-iterator) goes for now.

	var c = jsoniter.Config{
		EscapeHTML:              true,
		SortMapKeys:             false,
		MarshalFloatWith6Digits: true,
	}.Froze()

	geojson.CustomJSONMarshaler = c
	geojson.CustomJSONUnmarshaler = c

	// END OF json wah-wah...

	ctx := context.Background()

	e, err := emitter.NewEmitter(ctx, emitter_uri)

	if err != nil {
		log.Fatal(err)
	}

	defer e.Close()

	spatial_db, err := database.NewSpatialDatabase(ctx, spatial_database_uri)

	if err != nil {
		log.Fatal(err)
	}

	defer spatial_db.Disconnect(ctx)

	var properties_reader reader.Reader
	properties_reader = spatial_db

	if properties_reader_uri != "{spatial-database-uri}" {

		r, err := reader.NewReader(ctx, properties_reader_uri)

		if err != nil {
			log.Fatal(err)
		}

		properties_reader = r
	}

	inputs := &filter.SPRInputs{}
	inputs.IsCurrent = []int64{1}

	results_cb := hierarchy_filter.FirstButForgivingSPRResultsFunc

	resolver_opts := &hierarchy.PointInPolygonHierarchyResolverOptions{
		Database: spatial_db,
	}

	resolver, err := hierarchy.NewPointInPolygonHierarchyResolver(ctx, resolver_opts)

	if err != nil {
		log.Fatal(err)
	}

	mu := new(sync.RWMutex)
	wg := new(sync.WaitGroup)

	throttle := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		throttle <- true
	}

	parent_cache, err := ristretto.NewCache(&ristretto.Config[string, string]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})

	if err != nil {
		log.Fatal(err)
	}

	var csv_wr *csvdict.Writer

	i64_to_string := func(i64_list []int64) []string {

		str_list := make([]string, len(i64_list))

		for i, id := range i64_list {
			str_list[i] = strconv.FormatInt(id, 10)
		}

		return str_list
	}

	i64_to_csv := func(i64_list []int64) string {
		return strings.Join(i64_to_string(i64_list), ",")
	}

	process_place := func(ctx context.Context, pl *places.Place) error {

		parent_id := int64(-1)
		belongs_to := make([]int64, 0)
		str_hierarchies := ""

		defer func() {

			out := map[string]string{
				"4sq:id":          pl.Id,
				"wof:parent_id":   strconv.FormatInt(parent_id, 10),
				"wof:belongs_to":  i64_to_csv(belongs_to),
				"wof:hierarchies": str_hierarchies,
			}

			mu.Lock()
			defer mu.Unlock()

			if csv_wr == nil {

				wr, err := csvdict.NewWriter(os.Stdout)

				if err != nil {
					slog.Error("Failed to create CSV writer", "error", err)
					return
				}

				csv_wr = wr
			}

			csv_wr.WriteRow(out)
			csv_wr.Flush()
		}()

		lat := pl.Latitude
		lon := pl.Longitude

		pt := orb.Point([2]float64{lon, lat})
		f := geojson.NewFeature(pt)

		f.Properties["wof:id"] = pl.Id
		f.Properties["wof:name"] = pl.Name
		f.Properties["wof:placetype"] = "venue"
		f.Properties["lbl:latitude"] = lat
		f.Properties["lbl:longitude"] = lon

		body, err := f.MarshalJSON()

		if err != nil {
			slog.Error("Failed to marshal JSON", "error", err)
			return err
		}

		possible, err := resolver.PointInPolygon(ctx, inputs, body)

		if err != nil {
			slog.Error("Failed to resolve PIP", "error", err)
			return err
		}

		parent_spr, err := results_cb(ctx, spatial_db, body, possible)

		if err != nil {
			slog.Error("Failed to process results", "error", err)
			return err
		}

		if parent_spr != nil {

			p_id, err := strconv.ParseInt(parent_spr.Id(), 10, 64)

			if err != nil {
				slog.Error("Failed to parse parse parent ID", "id", parent_spr.Id(), "error", err)
				return err
			}

			parent_id = p_id

			k := parent_spr.Id()

			v, exists := parent_cache.Get(k)

			if exists {
				// slog.Info("GET", "k", k, "v", v)
				str_hierarchies = v
			} else {

				belongs_to = parent_spr.BelongsTo()

				parent_body, err := wof_reader.LoadBytes(ctx, properties_reader, p_id)

				if err != nil {
					slog.Warn("Failed to derive record from properties reader", "id", p_id, "error", err)
				} else {

					hierarchies := properties.Hierarchies(parent_body)

					candidates := []string{
						"microhood_id",
						"neighbourhood_id",
						"macrohood_id",
						"borough_id",
						"locality_id",
						"localadmin_id",
						"county_id",
						"region_id",
						"country_id",
						"continent_id",
						"empire_id",
					}

					str_hier := make([]string, len(hierarchies))

					for i, h := range hierarchies {

						// colon-separated list
						hier_csv := make([]string, len(candidates))

						for j, k := range candidates {

							id, exists := h[k]
							v := ""

							if exists {
								v = strconv.FormatInt(id, 10)
							}

							hier_csv[j] = v
						}

						str_hier[i] = strings.Join(hier_csv, ":")
					}

					str_hierarchies = strings.Join(str_hier, ",")
				}

				parent_cache.Set(k, str_hierarchies, 1)
			}
		}

		return nil
	}

	counter := int64(0)
	last_processed := int64(0)
	processed := int64(0)
	timing := int64(0)

	ticker := time.NewTicker(time.Duration(10) * time.Second)
	defer ticker.Stop()

	start := time.Now()

	go func() {

		for {
			select {
			case <-ticker.C:

				p := atomic.LoadInt64(&processed)
				diff := int64(0)

				if last_processed > 0 {
					diff = p - last_processed
				}

				last_processed = p

				slog.Info("Status", "counter", counter, "processed", p, "diff", diff, "avg t2p", float64(timing)/float64(p), "elaspsed", time.Since(start))
			}
		}
	}()

	for pl, err := range e.Emit(ctx) {

		counter += 1

		if err != nil {
			slog.Error("Failed to yield place", "error", err)
			continue
		}

		if start_after > 0 && start_after > counter {
			slog.Debug("Start after throttle", "after", start_after, "count", counter)
			continue
		}

		<-throttle

		wg.Add(1)

		go func(pl *places.Place) {

			t1 := time.Now()

			defer func() {
				throttle <- true

				t2 := time.Since(t1)
				atomic.AddInt64(&timing, t2.Milliseconds())
				atomic.AddInt64(&processed, 1)

				wg.Done()
			}()

			err = process_place(ctx, pl)

			if err != nil {
				slog.Error("Failed to process place", "error", err)
			}
		}(pl)
	}

	wg.Wait()

}
