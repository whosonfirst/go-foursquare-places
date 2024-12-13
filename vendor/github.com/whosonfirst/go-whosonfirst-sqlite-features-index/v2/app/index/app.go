package index

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"slices"

	"github.com/aaronland/go-sqlite/v2"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-reader"
	sql_tables "github.com/whosonfirst/go-whosonfirst-sql/tables"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features-index/v2"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/v2/tables"
	sql_index "github.com/whosonfirst/go-whosonfirst-sqlite-index/v4"
)

const index_alt_all string = "*"

func Run(ctx context.Context, logger *log.Logger) error {
	fs := DefaultFlagSet()
	return RunWithFlagSet(ctx, fs, logger)
}

// To do: Add RunWithOptions...

func RunWithFlagSet(ctx context.Context, fs *flag.FlagSet, logger *log.Logger) error {

	flagset.Parse(fs)

	runtime.GOMAXPROCS(procs)

	if spatial_tables {
		rtree = true
		geojson = true
		properties = true
		spr = true
	}

	if spelunker_tables {
		rtree = true
		spr = true
		geojson = true
		concordances = true
		ancestors = true
		search = true
	}
	
	db, err := sqlite.NewDatabase(ctx, db_uri)

	if err != nil {
		return fmt.Errorf("Unable to create database (%s) because %v", db_uri, err)
	}

	// optimize query performance
	// https://www.sqlite.org/pragma.html#pragma_optimize
	if optimize {

		defer func() {

			defer db.Close(ctx)

			conn, err := db.Conn(ctx)

			if err != nil {
				logger.Printf("Unable to optimize, because %v", err)
				return
			}

			_, err = conn.Exec("PRAGMA optimize")

			if err != nil {
				logger.Printf("Unable to optimize, because %v", err)
				return
			}
		}()

	} else {
		defer db.Close(ctx)
	}

	if live_hard {

		err = sqlite.LiveHardDieFast(ctx, db)

		if err != nil {
			return fmt.Errorf("Unable to live hard and die fast so just dying fast instead, because %v", err)
		}
	}

	to_index := make([]sqlite.Table, 0)

	if geojson || all {

		geojson_opts, err := tables.DefaultGeoJSONTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create '%s' table options because %s", sql_tables.GEOJSON_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, sql_tables.GEOJSON_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			geojson_opts.IndexAltFiles = true
		}

		gt, err := tables.NewGeoJSONTableWithDatabaseAndOptions(ctx, db, geojson_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.GEOJSON_TABLE_NAME, err)
		}

		to_index = append(to_index, gt)
	}

	if supersedes || all {

		t, err := tables.NewSupersedesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.SUPERSEDES_TABLE_NAME, err)
		}

		to_index = append(to_index, t)
	}

	if rtree || all {

		rtree_opts, err := tables.DefaultRTreeTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create 'rtree' table options because %s", err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, sql_tables.RTREE_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			rtree_opts.IndexAltFiles = true
		}

		gt, err := tables.NewRTreeTableWithDatabaseAndOptions(ctx, db, rtree_opts)

		if err != nil {
			return fmt.Errorf("failed to create 'rtree' table because %s", err)
		}

		to_index = append(to_index, gt)
	}

	if properties || all {

		properties_opts, err := tables.DefaultPropertiesTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create 'properties' table options because %s", err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, sql_tables.PROPERTIES_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			properties_opts.IndexAltFiles = true
		}

		gt, err := tables.NewPropertiesTableWithDatabaseAndOptions(ctx, db, properties_opts)

		if err != nil {
			return fmt.Errorf("failed to create 'properties' table because %s", err)
		}

		to_index = append(to_index, gt)
	}

	if spr || all {

		spr_opts, err := tables.DefaultSPRTableOptions()

		if err != nil {
			return fmt.Errorf("Failed to create '%s' table options because %v", sql_tables.SPR_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, sql_tables.SPR_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			spr_opts.IndexAltFiles = true
		}

		st, err := tables.NewSPRTableWithDatabaseAndOptions(ctx, db, spr_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.SPR_TABLE_NAME, err)
		}

		to_index = append(to_index, st)
	}

	if names || all {

		nm, err := tables.NewNamesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.NAMES_TABLE_NAME, err)
		}

		to_index = append(to_index, nm)
	}

	if ancestors || all {

		an, err := tables.NewAncestorsTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.ANCESTORS_TABLE_NAME, err)
		}

		to_index = append(to_index, an)
	}

	if concordances || all {

		cn, err := tables.NewConcordancesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", sql_tables.CONCORDANCES_TABLE_NAME, err)
		}

		to_index = append(to_index, cn)
	}

	// see the way we don't check all here - that's so people who don't have
	// spatialite installed can still use all (20180122/thisisaaronland)

	if geometries {

		geometries_opts, err := tables.DefaultGeometriesTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create '%s' table options because %v", sql_tables.GEOMETRIES_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, sql_tables.CONCORDANCES_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			geometries_opts.IndexAltFiles = true
		}

		gm, err := tables.NewGeometriesTableWithDatabaseAndOptions(ctx, db, geometries_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %v", sql_tables.CONCORDANCES_TABLE_NAME, err)
		}

		to_index = append(to_index, gm)
	}

	// see the way we don't check all here either - that's because this table can be
	// brutally slow to index and should probably really just be a separate database
	// anyway... (20180214/thisisaaronland)

	if search {

		// ALT FILES...

		st, err := tables.NewSearchTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create 'search' table because %v", err)
		}

		to_index = append(to_index, st)
	}

	if len(to_index) == 0 {
		return fmt.Errorf("You forgot to specify which (any) tables to index")
	}

	record_opts := &index.SQLiteFeaturesLoadRecordFuncOptions{
		StrictAltFiles: strict_alt_files,
	}

	record_func := index.SQLiteFeaturesLoadRecordFunc(record_opts)

	idx_opts := &sql_index.SQLiteIndexerOptions{
		DB:             db,
		Tables:         to_index,
		LoadRecordFunc: record_func,
	}

	if index_relations {

		r, err := reader.NewReader(ctx, relations_uri)

		if err != nil {
			return fmt.Errorf("Failed to load reader (%s), %v", relations_uri, err)
		}

		belongsto_func := index.SQLiteFeaturesIndexRelationsFunc(r)
		idx_opts.PostIndexFunc = belongsto_func
	}

	idx, err := sql_index.NewSQLiteIndexer(idx_opts)

	if err != nil {
		return fmt.Errorf("failed to create sqlite indexer because %v", err)
	}

	idx.Timings = timings
	idx.Logger = logger

	uris := fs.Args()

	err = idx.IndexURIs(ctx, iterator_uri, uris...)

	if err != nil {
		return fmt.Errorf("Failed to index paths in %s mode because: %s", iterator_uri, err)
	}

	return nil
}
