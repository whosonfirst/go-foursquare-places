package main

import (
	"context"
	"encoding/json"
	"flag"
	_ "fmt"
	"log"
	"log/slog"
	"os"

	"github.com/whosonfirst/go-foursquare-places/emitter"
)

func main() {

	var emitter_uri string

	flag.StringVar(&emitter_uri, "emitter-uri", "", "A registered /whosonfirst/go-foursquare-places/emitter.Emitter URI.")

	flag.Parse()

	ctx := context.Background()

	e, err := emitter.NewEmitter(ctx, emitter_uri)

	if err != nil {
		log.Fatal(err)
	}

	defer e.Close()

	for pl, err := range e.Emit(ctx) {

		if err != nil {
			slog.Error("Failed to yield place", "error", err)
			continue
		}

		enc := json.NewEncoder(os.Stdout)
		err = enc.Encode(pl)

		if err != nil {
			slog.Error("Failed to encode place", "error", err)
		}
	}
}
