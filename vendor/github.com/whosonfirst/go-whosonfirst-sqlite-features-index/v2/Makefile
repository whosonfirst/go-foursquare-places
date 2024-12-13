GOMOD=$(shell test -f "go.work" && echo "readonly" || echo "vendor")
LDFLAGS=-s -w

cli:
	go build -mod $(GOMOD) -ldflags="$(LDFLAGS)" \
		-o bin/wof-sqlite-index-features \
		cmd/wof-sqlite-index-features/main.go
	go build -mod $(GOMOD) -ldflags="$(LDFLAGS)" \
		-o bin/wof-sqlite-index-features-mattn \
		-tags "icu json1 fts5" \
		cmd/wof-sqlite-index-features-mattn/main.go

debug:
	./bin/wof-sqlite-index-features-mattn \
		-timings \
		-database-uri mattn:///usr/local/data/ca-3.db \
		-rtree \
		-spr \
		-geojson \
		-concordances \
		-search \
		-index-alt geojson \
		/usr/local/data/whosonfirst-data-admin-ca
