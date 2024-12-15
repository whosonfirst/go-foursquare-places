package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	database_sql "github.com/sfomuseum/go-database/sql"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
)

// IndexerPostIndexFunc is a custom function to invoke after a record has been indexed.
type IndexerPostIndexFunc func(context.Context, *sql.DB, []database_sql.Table, interface{}) error

// IndexerLoadRecordFunc is a custom `whosonfirst/go-whosonfirst-iterate/v2` callback function to be invoked
// for each record processed by the `IndexURIs` method.
type IndexerLoadRecordFunc func(context.Context, string, io.ReadSeeker, ...interface{}) (interface{}, error)

// Indexer is a struct that provides methods for indexing records in one or more SQLite database_sql.tables
type Indexer struct {
	// iterator_callback is the `whosonfirst/go-whosonfirst-iterate/v2` callback function used by the `IndexPaths` method
	iterator_callback emitter.EmitterCallbackFunc
	table_timings     map[string]time.Duration
	mu                *sync.RWMutex
	// Timings is a boolean flag indicating whether timings (time to index records) should be recorded)
	Timings bool
}

// IndexerOptions
type IndexerOptions struct {
	// DB is the `database_sql.sql.DB` instance that records will be indexed in.
	DB *sql.DB
	// Tables is the list of `sfomuseum/go-database_sql.Table` instances that records will be indexed in.
	Tables []database_sql.Table
	// LoadRecordFunc is a custom `whosonfirst/go-whosonfirst-iterate/v2` callback function to be invoked
	// for each record processed by	the `IndexURIs`	method.
	LoadRecordFunc IndexerLoadRecordFunc
	// PostIndexFunc is an optional custom function to invoke after a record has been indexed.
	PostIndexFunc IndexerPostIndexFunc
}

// NewSQLiteInder returns a `Indexer` configured with 'opts'.
func NewIndexer(opts *IndexerOptions) (*Indexer, error) {

	db := opts.DB
	tables := opts.Tables
	record_func := opts.LoadRecordFunc

	table_timings := make(map[string]time.Duration)
	mu := new(sync.RWMutex)

	iterator_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		logger := slog.Default()
		logger = logger.With("path", path)

		/*
			t1 := time.Now()

			defer func() {
				logger.Debug("Time to index record", "time", time.Since(t1))
			}()
		*/

		record, err := record_func(ctx, path, r, args...)

		if err != nil {
			logger.Error("Failed to load record", "error", err)
			return err
		}

		if record == nil {
			logger.Debug("Record func returned nil")
			return nil
		}

		mu.Lock()
		defer mu.Unlock()

		for _, t := range tables {

			logger := slog.Default()
			logger = logger.With("path", path)
			logger = logger.With("table", t.Name())

			t1 := time.Now()

			err = t.IndexRecord(ctx, db, record)

			if err != nil {
				logger.Error("Failed to index feature", "error", err)
				return err
			}

			t2 := time.Since(t1)

			n := t.Name()

			_, ok := table_timings[n]

			if ok {
				table_timings[n] += t2
			} else {
				table_timings[n] = t2
			}
		}

		if opts.PostIndexFunc != nil {

			err := opts.PostIndexFunc(ctx, db, tables, record)

			if err != nil {
				logger.Error("Post-index function failed", "error", err)
				return err
			}
		}

		return nil
	}

	i := Indexer{
		iterator_callback: iterator_cb,
		table_timings:     table_timings,
		mu:                mu,
		Timings:           false,
	}

	return &i, nil
}

// IndexURIs will index records returned by the `whosonfirst/go-whosonfirst-iterate` instance for 'uris',
func (idx *Indexer) IndexURIs(ctx context.Context, iterator_uri string, uris ...string) error {

	iter, err := iterator.NewIterator(ctx, iterator_uri, idx.iterator_callback)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator, %w", err)
	}

	done_ch := make(chan bool)
	t1 := time.Now()

	// ideally this could be a proper stand-along package method but then
	// we have to set up a whole bunch of scaffolding just to pass 'indexer'
	// around so... we're not doing that (20180205/thisisaaronland)

	show_timings := func() {

		t2 := time.Since(t1)

		i := atomic.LoadInt64(&iter.Seen)

		idx.mu.RLock()
		defer idx.mu.RUnlock()

		for t, d := range idx.table_timings {
			slog.Info("Time to index table", "table", t, "count", i, "time", d)
		}

		slog.Info("Time to index all", "count", i, "time", t2)
	}

	if idx.Timings {

		go func() {

			for {

				select {
				case <-done_ch:
					return
				case <-time.After(1 * time.Minute):
					show_timings()
				}
			}
		}()

		defer func() {
			done_ch <- true
		}()
	}

	err = iter.IterateURIs(ctx, uris...)

	if err != nil {
		return err
	}

	return nil
}
