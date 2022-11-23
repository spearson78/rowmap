package rowmap

import (
	"context"
	"database/sql"
)

// MapperFunc is used by query functions to map a row to an Entity
type MapperFunc[E any] func(row *sql.Rows) (E, error)

// Queryable enbales rowmap functions to be used with sql.DB sql.Conn and sql.Tx
type Queryable interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row

	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
