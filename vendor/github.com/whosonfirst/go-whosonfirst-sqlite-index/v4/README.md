# go-whosonfirst-sqlite-index

Go package for indexing SQLite databases using table constucts defined in the `aaronland/go-sqlite/v2` package and records defined by the `whosonfirst/go-whosonfirst-iterate/v2` package.

## Documentation

[![Go Reference](https://pkg.go.dev/badge/github.com/whosonfirst/go-whosonfirst-sqlite-index.svg)](https://pkg.go.dev/github.com/whosonfirst/go-whosonfirst-sqlite-index)

## Tools

```
$> make cli
go build -mod vendor -o bin/example cmd/example/main.go
```

### example

```
$> ./bin/example -h
Usage of ./bin/example:
  -database-uri string
    	 (default "modernc://mem")
  -emitter-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/v2 URI. Valid schemes are: directory://,featurecollection://,file://,filelist://,geojsonl://,null://,repo://. (default "repo://")
  -live-hard-die-fast
    	Enable various performance-related pragmas at the expense of possible (unlikely) database corruption (default true)
  -post-index
    	Enable post indexing callback function
  -timings
    	Display timings during and after indexing
```

For example:

```
$> ./bin/example -dsn 'modernc://cwd/test.db' /usr/local/data/sfomuseum-data-architecture/
2021/02/18 11:34:58 time to index paths (1) 403.514656ms

$> sqlite3  test.db 
SQLite version 3.28.0 2019-04-15 14:49:49
Enter ".help" for usage hints.
sqlite> .tables
example

sqlite> SELECT COUNT(id) FROM example;
12751
```

## See also

* https://github.com/aaronland/go-sqlite
* https://github.com/whosonfirst/go-whosonfirst-iterate
