package index

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	_ "github.com/aaronland/go-sqlite-modernc"
	"github.com/aaronland/go-sqlite/v2"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-feature/geometry"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	wof_tables "github.com/whosonfirst/go-whosonfirst-sqlite-features/v2/tables"
	sql_index "github.com/whosonfirst/go-whosonfirst-sqlite-index/v4"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

// SQLiteFeaturesLoadRecordFuncOptions is a struct to define options when loading Who's On First feature records.
type SQLiteFeaturesLoadRecordFuncOptions struct {
	// StrictAltFiles is a boolean flag indicating whether the failure to load or parse an alternate geometry file should trigger a critical error.
	StrictAltFiles bool
}

// SQLiteFeaturesIndexRelationsFuncOptions
type SQLiteFeaturesIndexRelationsFuncOptions struct {
	// Reader is a valid `whosonfirst/go-reader` instance used to load Who's On First feature data
	Reader reader.Reader
	// Strict is a boolean flag indicating whether the failure to load or parse feature record should trigger a critical error.
	Strict bool
}

// SQLiteFeaturesLoadRecordFunc returns a `go-whosonfirst-sqlite-index/v3.SQLiteIndexerLoadRecordFunc` callback
// function that will ensure the the record being processed is a valid Who's On First GeoJSON Feature record.
func SQLiteFeaturesLoadRecordFunc(opts *SQLiteFeaturesLoadRecordFuncOptions) sql_index.SQLiteIndexerLoadRecordFunc {

	cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) (interface{}, error) {

		select {

		case <-ctx.Done():
			return nil, nil
		default:
			// pass
		}

		body, err := io.ReadAll(r)

		if err != nil {
			return nil, fmt.Errorf("Failed read %s, %w", path, err)
		}

		_, err = properties.Id(body)

		if err != nil {
			return nil, fmt.Errorf("Failed to derive wof:id for %s, %w", path, err)
		}

		_, err = geometry.Geometry(body)

		if err != nil {
			return nil, fmt.Errorf("Failed to derive geometry for %s, %w", path, err)
		}

		return body, nil
	}

	return cb
}

// SQLiteFeaturesIndexRelationsFunc returns a `go-whosonfirst-sqlite-index/v3.SQLiteIndexerPostIndexFunc` callback
// function used to index relations for a WOF record after that record has been successfully indexed.
func SQLiteFeaturesIndexRelationsFunc(r reader.Reader) sql_index.SQLiteIndexerPostIndexFunc {

	opts := &SQLiteFeaturesIndexRelationsFuncOptions{}
	opts.Reader = r

	return SQLiteFeaturesIndexRelationsFuncWithOptions(opts)
}

// SQLiteFeaturesIndexRelationsFuncWithOptions returns a `go-whosonfirst-sqlite-index/v3.SQLiteIndexerPostIndexFunc` callback
// function used to index relations for a WOF record after that record has been successfully indexed, but with custom
// `SQLiteFeaturesIndexRelationsFuncOptions` options defined in 'opts'.
func SQLiteFeaturesIndexRelationsFuncWithOptions(opts *SQLiteFeaturesIndexRelationsFuncOptions) sql_index.SQLiteIndexerPostIndexFunc {

	seen := new(sync.Map)

	cb := func(ctx context.Context, db sqlite.Database, tables []sqlite.Table, record interface{}) error {

		geojson_t, err := wof_tables.NewGeoJSONTable(ctx)

		if err != nil {
			return fmt.Errorf("Failed to create new GeoJSON table, %w", err)
		}

		conn, err := db.Conn(ctx)

		if err != nil {
			return fmt.Errorf("Failed to establish database connection, %v", err)
		}

		body := record.([]byte)

		relations := make(map[int64]bool)

		candidates := []string{
			"properties.wof:belongsto",
			"properties.wof:involves",
			"properties.wof:depicts",
		}

		for _, path := range candidates {

			// log.Println("RELATIONS", path)

			rsp := gjson.GetBytes(body, path)

			if !rsp.Exists() {
				// log.Println("MISSING", path)
				continue
			}

			for _, r := range rsp.Array() {

				id := r.Int()

				// skip -1, -4, etc.
				// (20201224/thisisaaronland)

				if id <= 0 {
					continue
				}

				relations[id] = true
			}
		}

		for id, _ := range relations {

			_, ok := seen.Load(id)

			if ok {
				continue
			}

			seen.Store(id, true)

			sql := fmt.Sprintf("SELECT COUNT(id) FROM %s WHERE id=?", geojson_t.Name())
			row := conn.QueryRow(sql, id)

			var count int
			err = row.Scan(&count)

			if err != nil {
				return fmt.Errorf("Failed to count records for ID %d, %v", id, err)
			}

			if count != 0 {
				continue
			}

			rel_path, err := uri.Id2RelPath(id)

			if err != nil {
				return fmt.Errorf("Failed to determine relative path for %d, %v", id, err)
			}

			fh, err := opts.Reader.Read(ctx, rel_path)

			if err != nil {

				if opts.Strict {
					return fmt.Errorf("Failed to open %s, %v", rel_path, err)
				}

				slog.Debug("Failed to read '%s' because '%v'. Strict mode is disabled so skipping\n", rel_path, err)
				continue
			}

			defer fh.Close()

			ancestor, err := io.ReadAll(fh)

			if err != nil {
				return fmt.Errorf("Failed to read data for %s, %v", rel_path, err)
			}

			for _, t := range tables {

				err = t.IndexRecord(ctx, db, ancestor)

				if err != nil {
					return fmt.Errorf("Failed to index ancestor (%s), %v", rel_path, err)
				}
			}
		}

		return nil
	}

	return cb
}
