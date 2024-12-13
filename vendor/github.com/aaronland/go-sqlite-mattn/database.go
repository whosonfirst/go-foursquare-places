package mattn

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aaronland/go-sqlite/v2"
	"github.com/aaronland/go-sqlite/v2/database"	
	_ "github.com/mattn/go-sqlite3"
)

const SQLITE_SCHEME string = "mattn"
const SQLITE_DRIVER string = "sqlite3"

func init() {
	ctx := context.Background()
	sqlite.RegisterDatabase(ctx, SQLITE_SCHEME, NewMattnDatabase)
}

func NewMattnDatabase(ctx context.Context, db_uri string) (sqlite.Database, error) {

	dsn, err := database.DSNFromURI(db_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	conn, err := sql.Open(SQLITE_DRIVER, dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to open database connection, %w", err)
	}

	return database.NewAnyDatabase(ctx, dsn, conn)
}
