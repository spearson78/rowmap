package rowmap

import "context"

// QueryContext executes a query, typically a SELECT that returns entities using the provided mapper function. The args are for any placeholder parameters in the query. If zero rows are selected the returned slice will be nil.
func QueryContext[E any](ctx context.Context, db Queryable, mapper MapperFunc[E], sql string, p ...any) ([]E, error) {
	rows, err := db.QueryContext(ctx, sql, p...)
	return mapRows(mapper, rows, err)
}

// Query executes a query, typically a SELECT that returns entities using the provided mapper function. The args are for any placeholder parameters in the query. If zero rows are selected the returned slice will be nil.
//
// Query uses context.Background internally; to specify the context, use QueryContext.
func Query[E any](db Queryable, mapper MapperFunc[E], sql string, p ...any) ([]E, error) {
	return QueryContext(context.Background(), db, mapper, sql, p...)
}

// QueryRowContext executes a query that is expected oreturn at most one row. The result row is mapped to an entity using the provided mapper function. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
func QueryRowContext[E any](ctx context.Context, db Queryable, mapper MapperFunc[E], sql string, p ...any) (E, error) {
	rows, err := db.QueryContext(ctx, sql, p...)
	return mapSingleRow(mapper, rows, err)
}

// QueryRow executes a query that is expected oreturn at most one row. The result row is mapped to an entity using the provided mapper function. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
//
// QueryRow uses context.Background internally; to specify the context, use QueryRowContext.
func QueryRow[E any](db Queryable, mapper MapperFunc[E], sql string, p ...any) (E, error) {
	return QueryRowContext(context.Background(), db, mapper, sql, p...)
}
