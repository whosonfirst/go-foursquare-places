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
	"sync"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/sfomuseum/go-csvdict"
	"github.com/whosonfirst/go-foursquare-places"
	"github.com/whosonfirst/go-foursquare-places/emitter"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-pmtiles"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	hierarchy_filter "github.com/whosonfirst/go-whosonfirst-spatial/hierarchy/filter"
)

func main() {

	var spatial_database_uri string
	var emitter_uri string
	var workers int

	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "", "A registered whosonfirst/go-whosonfirst-spatial/database/SpatialDatabase URI to use for perforning reverse geocoding tasks.")
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

	process_place := func(ctx context.Context, pl *places.Place) error {

		parent_id := int64(-1)
		belongs_to := make([]int64, 0)

		defer func() {

			str_belongs_to := make([]string, len(belongs_to))

			for i, id := range belongs_to {
				str_belongs_to[i] = strconv.FormatInt(id, 10)
			}

			out := map[string]string{
				"4sq:id":         pl.Id,
				"wof:parent_id":  strconv.FormatInt(parent_id, 10),
				"wof:belongs_to": strings.Join(str_belongs_to, ","),
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
