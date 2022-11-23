package rowmap

import (
	"context"
	"database/sql"
)

//MappedStmt is a prepared statement. A MappedStmt is safe for concurrent use by multiple goroutines.

// If a MappedStmt is prepared on a Tx or Conn, it will be bound to a single underlying connection forever. If the Tx or Conn closes, the MappedStmt will become unusable and all operations will return an error. If a MappedStmt is prepared on a DB, it will remain usable for the lifetime of the DB. When the Stmt needs to execute on a new underlying connection, it will prepare itself on the new connection automatically.
type MappedStmt[E any] struct {
	stmt   *sql.Stmt
	mapper MapperFunc[E]
}

// Close closes the MappedStmt
func (m *MappedStmt[E]) Close() error {
	return m.stmt.Close()
}

// QueryContext executes a query, typically a SELECT that returns entities using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If zero rows are selected the returned slice will be nil.
func (m *MappedStmt[E]) QueryContext(ctx context.Context, args ...any) ([]E, error) {
	rows, err := m.stmt.QueryContext(ctx, args...)
	return mapRows(m.mapper, rows, err)
}

// Query executes a query, typically a SELECT that returns entities using the mapper function provided when preparing the statement.
func (m *MappedStmt[E]) Query(args ...any) ([]E, error) {
	return m.QueryContext(context.Background(), args...)
}

// QueryRowContext executes a query that is expected to return at most one row. The result row is mapped to an entity using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
func (m *MappedStmt[E]) QueryRowContext(ctx context.Context, args ...any) (E, error) {
	rows, err := m.stmt.QueryContext(ctx, args...)
	return mapSingleRow(m.mapper, rows, err)
}

// QueryRow executes a query that is expected to return at most one row. The result row is mapped to an entity using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
func (m *MappedStmt[E]) QueryRow(args ...any) (E, error) {
	return m.QueryRowContext(context.Background(), args...)
}

// PrepareContext creates a prepared statement for later queries whose results will be mapped using the provided MapperFunc. Multiple queries or executions may be run concurrently from the returned statement. The caller must call the statement's Close method when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the execution of the statement.
func PrepareContext[E any](ctx context.Context, db Queryable, mapper MapperFunc[E], query string) (*MappedStmt[E], error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &MappedStmt[E]{
		stmt:   stmt,
		mapper: mapper,
	}, nil
}

// PrepareContext creates a prepared statement for later queries whose results will be mapped using the provided MapperFunc. Multiple queries or executions may be run concurrently from the returned statement. The caller must call the statement's Close method when the statement is no longer needed.
func Prepare[E any](db Queryable, mapper MapperFunc[E], query string) (*MappedStmt[E], error) {
	return PrepareContext(context.Background(), db, mapper, query)
}
