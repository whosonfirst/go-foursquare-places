package index

import (
	"context"
	"fmt"
	"github.com/aaronland/go-sqlite/v2"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// SQLiteIndexerPostIndexFunc is a custom function to invoke after a record has been indexed.
type SQLiteIndexerPostIndexFunc func(context.Context, sqlite.Database, []sqlite.Table, interface{}) error

// SQLiteIndexerLoadRecordFunc is a custom `whosonfirst/go-whosonfirst-iterate/v2` callback function to be invoked
// for each record processed by the `IndexURIs` method.
type SQLiteIndexerLoadRecordFunc func(context.Context, string, io.ReadSeeker, ...interface{}) (interface{}, error)

// SQLiteIndexer is a struct that provides methods for indexing records in one or more SQLite database tables
type SQLiteIndexer struct {
	// iterator_callback is the `whosonfirst/go-whosonfirst-iterate/v2` callback function used by the `IndexPaths` method
	iterator_callback emitter.EmitterCallbackFunc
	table_timings     map[string]time.Duration
	mu                *sync.RWMutex
	// Timings is a boolean flag indicating whether timings (time to index records) should be recorded)
	Timings bool
	// Logger is a `log.Logger` instance
	Logger *log.Logger
}

// SQLiteIndexerOptions
type SQLiteIndexerOptions struct {
	// DB is the `aaronland/go-sqlite.Database` instance that records will be indexed in.
	DB sqlite.Database
	// Tables is the list of `aaronland/go-sqlite.Table` instances that records will be indexed in.
	Tables []sqlite.Table
	// LoadRecordFunc is a custom `whosonfirst/go-whosonfirst-iterate/v2` callback function to be invoked
	// for each record processed by	the `IndexURIs`	method.
	LoadRecordFunc SQLiteIndexerLoadRecordFunc
	// PostIndexFunc is an optional custom function to invoke after a record has been indexed.
	PostIndexFunc SQLiteIndexerPostIndexFunc
}

// NewSQLiteInder returns a `SQLiteIndexer` configured with 'opts'.
func NewSQLiteIndexer(opts *SQLiteIndexerOptions) (*SQLiteIndexer, error) {

	db := opts.DB
	tables := opts.Tables
	record_func := opts.LoadRecordFunc

	table_timings := make(map[string]time.Duration)
	mu := new(sync.RWMutex)

	logger := log.Default()

	iterator_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		record, err := record_func(ctx, path, r, args...)

		if err != nil {
			logger.Printf("Failed to load record (%s) because %s", path, err)
			return err
		}

		if record == nil {
			return nil
		}

		db.Lock(ctx)
		defer db.Unlock(ctx)

		for _, t := range tables {

			t1 := time.Now()

			err = t.IndexRecord(ctx, db, record)

			if err != nil {
				logger.Printf("Failed to index feature (%s) in '%s' table because %s", path, t.Name(), err)
				return err
			}

			t2 := time.Since(t1)

			n := t.Name()

			mu.Lock()

			_, ok := table_timings[n]

			if ok {
				table_timings[n] += t2
			} else {
				table_timings[n] = t2
			}

			mu.Unlock()
		}

		if opts.PostIndexFunc != nil {

			err := opts.PostIndexFunc(ctx, db, tables, record)

			if err != nil {
				return err
			}
		}

		return nil
	}

	i := SQLiteIndexer{
		iterator_callback: iterator_cb,
		table_timings:     table_timings,
		mu:                mu,
		Timings:           false,
		Logger:            logger,
	}

	return &i, nil
}

// IndexPaths is deprecated and has been superseded by the `IndexURIs` method.
func (idx *SQLiteIndexer) IndexPaths(ctx context.Context, iterator_uri string, uris []string) error {
	idx.Logger.Println("The IndexPaths method is deprecated. Please use IndexURIs instead.")
	return idx.IndexURIs(ctx, iterator_uri, uris...)
}

// IndexURIs will index records returned by the `whosonfirst/go-whosonfirst-iterate` instance for 'uris',
func (idx *SQLiteIndexer) IndexURIs(ctx context.Context, iterator_uri string, uris ...string) error {

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
			idx.Logger.Printf("Time to index %s (%d) : %v", t, i, d)
		}

		idx.Logger.Printf("Time to index all (%d) : %v", i, t2)
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
