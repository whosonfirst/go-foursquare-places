package main

import (
	"compress/bzip2"
	"context"
	"flag"
	_ "fmt"
	"log"
	"log/slog"
	"os"

	"github.com/aaronland/go-foursquare-places"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/hierarchy"
	hierarchy_filter "github.com/whosonfirst/go-whosonfirst-spatial/hierarchy/filter"
)

func main() {

	var spatial_database_uri string

	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "", "...")

	flag.Parse()

	ctx := context.Background()

	var data_r reader.Reader

	spatial_db, err := database.NewSpatialDatabase(ctx, spatial_database_uri)

	if err != nil {
		log.Fatal(err)
	}

	var inputs *filter.SPRInputs
	var results_cb hierarchy_filter.FilterSPRResultsFunc

	resolver_opts := &hierarchy.PointInPolygonHierarchyResolverOptions{
		Database: spatial_db,
	}

	resolver, err := hierarchy.NewPointInPolygonHierarchyResolver(ctx, resolver_opts)

	for _, path := range flag.Args() {

		r, err := os.Open(path)

		if err != nil {
			log.Fatal(err)
		}

		br := bzip2.NewReader(r)

		for pl, err := range places.Emit(ctx, br) {

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

			parent_spr, err := results_cb(ctx, data_r, body, possible)

			if err != nil {
				slog.Error("Failed to process results", "error", err)
				continue
			}

			slog.Info("id", pl.Id, "parent", parent_spr.Id)
		}
	}
}