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
)

func main() {

	flag.Parse()

	ctx := context.Background()

	for _, path := range flag.Args() {

		r, err := os.Open(path)

		if err != nil {
			log.Fatal(err)
		}

		br := bzip2.NewReader(r)

		for pl, err := range places.Emit(ctx, br) {

			if err != nil {
				slog.Error("Failed to yield place", "error", err)
			}

			slog.Debug("Place", "place", pl)
		}
	}
}
