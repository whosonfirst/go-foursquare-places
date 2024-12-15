package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	_ "log/slog"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/filters"
)

func init() {
	ctx := context.Background()
	emitter.RegisterEmitter(ctx, "sql", NewSQLiteEmitter)
}

// SQLiteEmitter implements the `Emitter` interface for crawling records in a SQLite database (specifically a SQLite database with a 'geojson' table produced by `whosonfirst/go-whosonfirst-sqlite-features` and `whosonfirst/go-whosonfirst-sqlite-features-index`).
type SQLiteEmitter struct {
	emitter.Emitter
	engine string
	// filters is a `whosonfirst/go-whosonfirst-iterate/v32/filters.Filters` instance used to include or exclude specific records from being crawled.
	filters filters.Filters
	// throttle is a channel used to control the maximum number of database rows that will be processed simultaneously.
	throttle chan bool
}

// NewGitEmitter() returns a new `GitEmitter` instance configured by 'uri' in the form of:
//
//	sqlite://?{PARAMETERS}
//
// {PARAMETERS} may be:
// * `?include=` Zero or more `aaronland/go-json-query` query strings containing rules that must match for a document to be considered for further processing.
// * `?exclude=` Zero or more `aaronland/go-json-query`	query strings containing rules that if matched will prevent a document from being considered for further processing.
// * `?include_mode=` A valid `aaronland/go-json-query` query mode string for testing inclusion rules.
// * `?exclude_mode=` A valid `aaronland/go-json-query` query mode string for testing exclusion rules.
// * `?processes=` An optional number assigning the maximum number of database rows that will be processed simultaneously. (Default is defined by `runtime.NumCPU()`.)
func NewSQLiteEmitter(ctx context.Context, uri string) (emitter.Emitter, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	max_procs := runtime.NumCPU()

	if q.Get("processes") != "" {

		procs, err := strconv.ParseInt(q.Get("processes"), 10, 64)

		if err != nil {
			return nil, fmt.Errorf("Failed to parse 'processes' parameter, %w", err)
		}

		max_procs = int(procs)
	}

	throttle_ch := make(chan bool, max_procs)

	for i := 0; i < max_procs; i++ {
		throttle_ch <- true
	}

	f, err := filters.NewQueryFiltersFromQuery(ctx, q)

	if err != nil {
		return nil, fmt.Errorf("Failed to create query filters, %w", err)
	}

	em := &SQLiteEmitter{
		engine:   u.Host,
		filters:  f,
		throttle: throttle_ch,
	}

	return em, nil
}

// WalkURI() walks (crawls) the SQLite database identified by 'uri' and for each file (not excluded by any filters specified
// when `idx` was created) invokes 'index_cb'.
func (d *SQLiteEmitter) WalkURI(ctx context.Context, emitter_cb emitter.EmitterCallbackFunc, uri string) error {

	conn, err := sql.Open(d.engine, uri)

	if err != nil {
		return err
	}

	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "SELECT id, body FROM geojson")

	if err != nil {
		return fmt.Errorf("Failed to query 'geojson' table with '%s', %w", uri, err)
	}

	sqlite_ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	error_ch := make(chan error)

	wg := new(sync.WaitGroup)

	for rows.Next() {

		<-d.throttle

		var wofid int64
		var body string

		err := rows.Scan(&wofid, &body)

		if err != nil {
			return fmt.Errorf("Failed to scan row with '%s', %w", uri, err)
		}

		wg.Add(1)

		go func(ctx context.Context, wofid int64, body string) {

			defer func() {
				d.throttle <- true
				wg.Done()
			}()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			// uri := fmt.Sprintf("sqlite://%s#geojson:%d", path, wofid)

			// see the way we're passing in STDIN and not uri as the path?
			// that because we call ctx, err := ContextForPath(path) in the
			// process() method and since uri won't be there nothing will
			// get indexed - it's not ideal it's just what it is today...
			// (20171213/thisisaaronland)

			sr := strings.NewReader(body)

			fh, err := ioutil.NewReadSeekCloser(sr)

			if err != nil {
				error_ch <- fmt.Errorf("Failed to create ReadSeekCloser for record '%d' with '%s', %w", wofid, uri, err)
				return
			}

			if d.filters != nil {

				ok, err := d.filters.Apply(ctx, fh)

				if err != nil {
					error_ch <- fmt.Errorf("Failed to apply query filters to record '%d' with '%s', %w", wofid, uri, err)
					return
				}

				if !ok {
					return
				}

				_, err = fh.Seek(0, 0)

				if err != nil {
					error_ch <- fmt.Errorf("Failed to reset filehandle for record '%d' with '%s', %w", wofid, uri, err)
					return

				}
			}

			err = emitter_cb(ctx, emitter.STDIN, fh)

			if err != nil {
				error_ch <- fmt.Errorf("Indexing callback failed for record '%d' with '%s', %w", wofid, uri, err)
			}

		}(sqlite_ctx, wofid, body)

		select {
		case e := <-error_ch:
			cancel()
			return e
		default:
			// pass
		}
	}

	wg.Wait()

	err = rows.Err()

	if err != nil {
		return fmt.Errorf("Database reported an error scanning rows with '%s', %w", uri, err)
	}

	return nil
}
