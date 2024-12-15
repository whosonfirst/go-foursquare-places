package index

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"runtime"
	"slices"

	database_sql "github.com/sfomuseum/go-database/sql"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-whosonfirst-database/sql/indexer"
	"github.com/whosonfirst/go-whosonfirst-database/sql/tables"
)

const index_alt_all string = "*"

func Run(ctx context.Context) error {
	fs := DefaultFlagSet()
	return RunWithFlagSet(ctx, fs)
}

// To do: Add RunWithOptions...

func RunWithFlagSet(ctx context.Context, fs *flag.FlagSet) error {

	flagset.Parse(fs)

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Verbose logging enabled")
	}

	runtime.GOMAXPROCS(procs)

	if spatial_tables {
		rtree = true
		geojson = true
		properties = true
		spr = true
	}

	if spelunker_tables {
		// rtree = true
		spr = true
		spelunker = true
		geojson = true
		concordances = true
		ancestors = true
		search = true

		to_index_alt := []string{
			tables.GEOJSON_TABLE_NAME,
		}

		for _, table_name := range to_index_alt {

			if !slices.Contains(index_alt, table_name) {
				index_alt = append(index_alt, table_name)
			}
		}

	}

	logger := slog.Default()

	db, err := database_sql.OpenWithURI(ctx, db_uri)

	if err != nil {
		return err
	}

	defer func() {

		err := db.Close()

		if err != nil {
			logger.Error("Failed to close database connection", "error", err)
		}
	}()

	db_driver := database_sql.Driver(db)

	switch db_driver {
	case database_sql.POSTGRES_DRIVER:

		if spatial_tables {
			rtree = false
			geometries = true
		}

		if rtree {
			return fmt.Errorf("-rtree table not supported by the %s driver", db_driver)
		}

		if search {
			return fmt.Errorf("-search table not (yet) supported by the %s driver", db_driver)
		}

	case database_sql.SQLITE_DRIVER:

		// optimize query performance
		// https://www.sqlite.org/pragma.html#pragma_optimize
		if optimize {

			defer func() {

				_, err = db.Exec("PRAGMA optimize")

				if err != nil {
					logger.Error("Failed to optimize", "error", err)
					return
				}
			}()

		}

	}

	to_index := make([]database_sql.Table, 0)

	if geojson || all {

		geojson_opts, err := tables.DefaultGeoJSONTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create '%s' table options because %s", tables.GEOJSON_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, tables.GEOJSON_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			geojson_opts.IndexAltFiles = true
		}

		gt, err := tables.NewGeoJSONTableWithDatabaseAndOptions(ctx, db, geojson_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.GEOJSON_TABLE_NAME, err)
		}

		to_index = append(to_index, gt)
	}

	if supersedes || all {

		t, err := tables.NewSupersedesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.SUPERSEDES_TABLE_NAME, err)
		}

		to_index = append(to_index, t)
	}

	if rtree || all {

		rtree_opts, err := tables.DefaultRTreeTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create 'rtree' table options because %s", err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, tables.RTREE_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
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

		if alt_files || slices.Contains(index_alt, tables.PROPERTIES_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
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
			return fmt.Errorf("Failed to create '%s' table options because %v", tables.SPR_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, tables.SPR_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			spr_opts.IndexAltFiles = true
		}

		st, err := tables.NewSPRTableWithDatabaseAndOptions(ctx, db, spr_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.SPR_TABLE_NAME, err)
		}

		to_index = append(to_index, st)
	}

	if spelunker || all {

		spelunker_opts, err := tables.DefaultSpelunkerTableOptions()

		if err != nil {
			return fmt.Errorf("Failed to create '%s' table options because %v", tables.SPELUNKER_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, tables.SPELUNKER_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			spelunker_opts.IndexAltFiles = true
		}

		st, err := tables.NewSpelunkerTableWithDatabaseAndOptions(ctx, db, spelunker_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.SPELUNKER_TABLE_NAME, err)
		}

		to_index = append(to_index, st)
	}

	if names || all {

		nm, err := tables.NewNamesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.NAMES_TABLE_NAME, err)
		}

		to_index = append(to_index, nm)
	}

	if ancestors || all {

		an, err := tables.NewAncestorsTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.ANCESTORS_TABLE_NAME, err)
		}

		to_index = append(to_index, an)
	}

	if concordances || all {

		cn, err := tables.NewConcordancesTableWithDatabase(ctx, db)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %s", tables.CONCORDANCES_TABLE_NAME, err)
		}

		to_index = append(to_index, cn)
	}

	// see the way we don't check all here - that's so people who don't have
	// spatialite installed can still use all (20180122/thisisaaronland)

	if geometries {

		geometries_opts, err := tables.DefaultGeometriesTableOptions()

		if err != nil {
			return fmt.Errorf("failed to create '%s' table options because %v", tables.GEOMETRIES_TABLE_NAME, err)
		}

		// alt_files is deprecated (20240229/straup)

		if alt_files || slices.Contains(index_alt, tables.CONCORDANCES_TABLE_NAME) || slices.Contains(index_alt, index_alt_all) {
			geometries_opts.IndexAltFiles = true
		}

		gm, err := tables.NewGeometriesTableWithDatabaseAndOptions(ctx, db, geometries_opts)

		if err != nil {
			return fmt.Errorf("failed to create '%s' table because %v", tables.CONCORDANCES_TABLE_NAME, err)
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

	record_opts := &indexer.LoadRecordFuncOptions{
		StrictAltFiles: strict_alt_files,
	}

	record_func := indexer.LoadRecordFunc(record_opts)

	idx_opts := &indexer.IndexerOptions{
		DB:             db,
		Tables:         to_index,
		LoadRecordFunc: record_func,
	}

	if index_relations {

		r, err := reader.NewReader(ctx, relations_uri)

		if err != nil {
			return fmt.Errorf("Failed to load reader (%s), %v", relations_uri, err)
		}

		belongsto_func := indexer.IndexRelationsFunc(r)
		idx_opts.PostIndexFunc = belongsto_func
	}

	idx, err := indexer.NewIndexer(idx_opts)

	if err != nil {
		return fmt.Errorf("failed to create sqlite indexer because %v", err)
	}

	idx.Timings = timings

	uris := fs.Args()

	err = idx.IndexURIs(ctx, iterator_uri, uris...)

	if err != nil {
		return fmt.Errorf("Failed to index paths in %s mode because: %s", iterator_uri, err)
	}

	return nil
}
