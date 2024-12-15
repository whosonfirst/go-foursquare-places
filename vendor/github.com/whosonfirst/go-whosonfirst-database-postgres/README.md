# go-whosonfirst-database-postgres

Go package providing tools for indexing Who's On First records in Postgres databases.

## Documentation

Documentation is incomplete at this time.

## Under the hood

This package is a very thin wrapper around the [whosonfirst/go-whosonfirst-database](https://github.com/whosonfirst/go-whosonfirst-database) package. That package provides all of the actual functionality for indexing Who's On First records but does NOT load any specific `database/sql` drivers. That happens in this package.

It uses the [lib/pq](https://github.com/lib/pq) database driver for interacting with Postgres.

## Tools

```
$> make cli
go build -mod vendor -ldflags="-s -w" -o bin/wof-sqlite-index cmd/wof-postgres-index/main.go
```

### wof-postgres-index

Index one or more Who's On First sources in a Postgres database.

```
$> ./bin/wof-postgres-index -h
  -all
    	Index all tables (except the 'search' and 'geometries' tables which you need to specify explicitly)
  -ancestors
    	Index the 'ancestors' tables
  -concordances
    	Index the 'concordances' tables
  -database-uri string
    	A URI in the form of 'sql://{DATABASE_SQL_ENGINE}?dsn={DATABASE_SQL_DSN}'. For example: 'sql://postgres?dsn=user=asc host=localhost dbname=whosonfirst sslmode=disable'
  -geojson
    	Index the 'geojson' table
  -geometries
    	Index the 'geometries' table (requires that libspatialite already be installed)
  -index-alt value
    	Zero or more table names where alt geometry files should be indexed.
  -index-alt-files
    	Index alt geometries. This flag is deprecated, please use -index-alt=TABLE,TABLE,etc. instead. To index alt geometries in all the applicable tables use -index-alt=*
  -index-relations
    	Index the records related to a feature, specifically wof:belongsto, wof:depicts and wof:involves. Alt files for relations are not indexed at this time.
  -index-relations-reader-uri string
    	A valid go-reader.Reader URI from which to read data for a relations candidate.
  -iterator-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/v2 URI. Supported emitter URI schemes are: directory://,featurecollection://,file://,filelist://,geojsonl://,null://,repo:// (default "repo://")
  -names
    	Index the 'names' table
  -optimize
    	Attempt to optimize the database before closing connection (default true)
  -processes int
    	The number of concurrent processes to index data with (default 20)
  -properties
    	Index the 'properties' table
  -rtree
    	Index the 'rtree' table
  -search
    	Index the 'search' table (using SQLite FTS4 full-text indexer)
  -spatial-tables
    	If true then index the necessary tables for use with the whosonfirst/go-whosonfirst-spatial-sqlite package.
  -spelunker
    	Index the 'spelunker' table
  -spelunker-tables
    	If true then index the necessary tables for use with the whosonfirst/go-whosonfirst-spelunker packages
  -spr
    	Index the 'spr' table
  -strict-alt-files
    	Be strict when indexing alt geometries (default true)
  -supersedes
    	Index the 'supersedes' table
  -timings
    	Display timings during and after indexing
  -verbose
    	Enable verbose (debug) logging
```

For example:

```
$> ./bin/wof-postgres-index \
	-database-uri 'sql://postgres?dsn=user=asc host=localhost dbname=whosonfirst sslmode=disable' \
	-timings -spatial-tables \
	/usr/local/data/whosonfirst-data-admin-us/

2024/12/14 13:09:19 INFO Time to index table table=geojson count=40089 time=14.284594424s
2024/12/14 13:09:19 INFO Time to index table table=properties count=40089 time=8.692767507s
2024/12/14 13:09:19 INFO Time to index table table=spr count=40089 time=16.922203837s
2024/12/14 13:09:19 INFO Time to index table table=geometries count=40089 time=19.79474847s
2024/12/14 13:09:19 INFO Time to index all count=40089 time=1m0.000138958s
...
2024/12/14 13:15:19 INFO Time to index table table=spr count=419039 time=2m1.527413667s
2024/12/14 13:15:19 INFO Time to index table table=geometries count=419039 time=1m53.67685761s
2024/12/14 13:15:19 INFO Time to index table table=geojson count=419039 time=1m46.150728117s
2024/12/14 13:15:19 INFO Time to index table table=properties count=419039 time=1m15.808681398s
2024/12/14 13:15:19 INFO Time to index all count=419039 time=7m0.005669541s
2024/12/14 13:15:31 time to index paths (1) 7m12.867244416s
```

## See also

* https://github.com/whosonfirst/go-whosonfirst-database/
* https://github.com/sfomuseum/go-database/
* https://github.com/lib/pq