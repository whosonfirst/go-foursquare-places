package main

import (
	"context"
	"flag"
	_ "fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"slices"
	"sync"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/sfomuseum/go-csvdict"
	"github.com/whosonfirst/go-foursquare-places"
	"github.com/whosonfirst/go-reader"
	wof_reader "github.com/whosonfirst/go-whosonfirst-reader"	
	"github.com/whosonfirst/go-foursquare-places/emitter"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"	
	hierarchy_filter "github.com/whosonfirst/go-whosonfirst-spatial/hierarchy/filter"
)

func main() {

	var spatial_database_uri string
	var properties_reader_uri string
	
	var emitter_uri string
	var workers int

	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "", "A registered whosonfirst/go-whosonfirst-spatial/database/SpatialDatabase URI to use for perforning reverse geocoding tasks.")
	flag.StringVar(&properties_reader_uri, "properties-reader-uri", "{spatial-database-uri}", "...")
	
	flag.StringVar(&emitter_uri, "emitter-uri", "", "A registered whosonfirst/go-foursquare-places/emitter.Emitter URI.")
	flag.IntVar(&workers, "workers", 100, "The maximum number of workers to process reverse geocoding tasks.")

	flag.Parse()

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

	defer spatial_db.Close(ctx)

	var properties_reader reader.Reader

	if properties_reader_uri != "" {

		use_spatial_uri := "{spatial-database-uri}"

		if properties_reader_uri == use_spatial_uri {
			properties_reader_uri = spatial_database_uri
		}

		r, err := reader.NewReader(ctx, properties_reader_uri)

		if err != nil {
			log.Fatal(err)
		}

		properties_reader = r
	}

	if properties_reader == nil {
		properties_reader = spatial_db
	}
	
	inputs := &filter.SPRInputs{}
	inputs.IsCurrent = []int64{1}

	results_cb := hierarchy_filter.FirstButForgivingSPRResultsFunc

	resolver_opts := &hierarchy.PointInPolygonHierarchyResolverOptions{
		Database: spatial_db,
	}

	resolver, err := hierarchy.NewPointInPolygonHierarchyResolver(ctx, resolver_opts)

	mu := new(sync.RWMutex)
	wg := new(sync.WaitGroup)

	throttle := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		throttle <- true
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

		neighbourhoods := make([]int64, 0)
		localities := make([]int64, 0)
		regions := make([]int64, 0)
		countries := make([]int64, 0)

		defer func() {

			str_belongs_to := make([]string, len(belongs_to))

			for i, id := range belongs_to {
				str_belongs_to[i] = strconv.FormatInt(id, 10)
			}

			out := map[string]string{
				"4sq:id":         pl.Id,
				"wof:parent_id":  strconv.FormatInt(parent_id, 10),
				"wof:belongs_to": i64_to_csv(belongs_to),
				"wof:neighbourhoods": i64_to_csv(neighbourhoods),
				"wof:localities": i64_to_csv(localities),
				"wof:regions": i64_to_csv(regions),
				"wof:countries": i64_to_csv(countries),				
			}

			mu.Lock()
			defer mu.Unlock()

			if csv_wr == nil {

				fieldnames := make([]string, 0)

				for k, _ := range out {
					fieldnames = append(fieldnames, k)
				}

				wr, err := csvdict.NewWriter(os.Stdout, fieldnames)

				if err != nil {
					slog.Error("Failed to create CSV writer", "error", err)
					return
				}

				csv_wr = wr
				csv_wr.WriteHeader()
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
			belongs_to = parent_spr.BelongsTo()

			parent_body, err := wof_reader.LoadBytes(ctx, properties_reader, p_id)

			if err != nil {

			} else {
				
				hierarchies := properties.Hierarchies(parent_body)

				foo := func(candidates map[string]int64, key string, target []int64) []int64 {

					id, exists := candidates[key]

					if exists {

						if !slices.Contains(target, id){
						   target = append(target, id)
						}
					}

					return target
				}
				
				for _, h := range hierarchies {

					neighbourhoods = foo(h, "neighbourhood_id", neighbourhoods)
					localities = foo(h, "locality_id", localities)
					regions = foo(h, "region_id", regions)
					countries = foo(h, "country_id", countries)					
				}
			}


		}

		return nil
	}

	for pl, err := range e.Emit(ctx) {

		if err != nil {
			slog.Error("Failed to yield place", "error", err)
			continue
		}

		<-throttle

		wg.Add(1)

		go func(pl *places.Place) {

			defer func() {
				throttle <- true
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
