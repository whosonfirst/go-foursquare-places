package main

import (
	"context"
	"flag"
	_ "fmt"
	"log"
	"log/slog"

	// "github.com/aaronland/go-foursquare-places"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/whosonfirst/go-foursquare-places/emitter"
	// "github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	hierarchy_filter "github.com/whosonfirst/go-whosonfirst-spatial/hierarchy/filter"
)

func main() {

	var spatial_database_uri string
	var emitter_uri string

	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "", "...")
	flag.StringVar(&emitter_uri, "emitter-uri", "", "")

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

	for pl, err := range e.Emit(ctx) {

		if err != nil {
			slog.Error("Failed to yield place", "error", err)
			continue
		}

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
			continue
		}

		possible, err := resolver.PointInPolygon(ctx, inputs, body)

		if err != nil {
			slog.Error("Failed to resolve PIP", "error", err)
			continue
		}

		parent_spr, err := results_cb(ctx, spatial_db, body, possible)

		if err != nil {
			slog.Error("Failed to process results", "error", err)
			continue
		}

		slog.Info("id", pl.Id, "parent", parent_spr)
	}
}
