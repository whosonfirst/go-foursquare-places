package main

import (
	_ "context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sfomuseum/go-csvdict/v2"
)

func main() {

	flag.Parse()

	countries := new(sync.Map)
	writers := make(map[string]*csvdict.Writer)

	counter := int64(0)

	workers := 200
	throttle := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		throttle <- true
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				slog.Info("Status", "count", atomic.LoadInt64(&counter))
			}
		}
	}()

	derive_country := func(id int64) (string, error) {

		v, exists := countries.Load(id)

		if exists {
			return v.(string), nil
		}

		url := fmt.Sprintf("https://spelunker.whosonfirst.org/select/%d?select=properties.wof:country", id)
		rsp, err := http.Get(url)

		if err != nil {
			return "", err
		}

		defer rsp.Body.Close()

		if rsp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("%s %d %s", url, rsp.StatusCode, rsp.Status)
		}

		body, err := io.ReadAll(rsp.Body)

		if err != nil {
			return "", err
		}

		country := strings.Trim(string(body), `"`)
		countries.LoadOrStore(id, country)

		// slog.Info("Country", "url", url, "code", country)
		return country, nil
	}

	for _, path := range flag.Args() {

		r, err := csvdict.NewReaderFromPath(path)

		if err != nil {
			log.Fatal(err)
		}

		wg := new(sync.WaitGroup)
		mu := new(sync.RWMutex)

		for row, err := range r.Iterate() {

			if err != nil {
				log.Fatal(err)
			}

			<-throttle
			wg.Add(1)

			go func(row map[string]string) {

				defer func() {
					atomic.AddInt64(&counter, 1)
					throttle <- true
					wg.Done()
				}()

				hiers := strings.Split(row["wof:hierarchies"], ",")

				for _, str_h := range hiers {

					country_id := int64(-1)
					str_country := "XY"

					h := strings.Split(str_h, ":")

					if len(h) >= 3 && h[3] != "" && h[3] != "-1" {
						str_country = h[3]

						v, err := strconv.ParseInt(str_country, 10, 64)

						if err != nil {
							slog.Error(err.Error())
						} else {

							country_id = v
							code, err := derive_country(country_id)

							if err != nil {
								slog.Error(err.Error())
							} else {
								str_country = code
							}
						}
					}

					mu.RLock()
					csv_wr, exists := writers[str_country]
					mu.RUnlock()

					if !exists {

						mu.Lock()

						csv_path := fmt.Sprintf("/Users/asc/data/foursquare-wof-sorted/foursquare-wof-%s.csv", str_country)
						wr, err := csvdict.NewWriterFromPath(csv_path)

						if err != nil {
							log.Fatal(err)
						}

						writers[str_country] = wr
						csv_wr = wr

						// slog.Info(csv_path)
						mu.Unlock()
					}

					// slog.Info("Write", "country", str_country, "id", row["4sq:id"])
					csv_wr.WriteRow(row)
				}

			}(row)
		}

		wg.Wait()

		for _, wr := range writers {
			wr.Flush()
		}

		slog.Info("Complete", "path", path, "count", atomic.LoadInt64(&counter))
	}
}
