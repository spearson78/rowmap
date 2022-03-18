package rowmap

import (
	"database/sql"
)

func mapRows[E any](mapper MapperFunc[E], rows *sql.Rows, err error) ([]E, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entities []E
	for rows.Next() {

		e, err := mapper(rows)
		if err != nil {
			return nil, err
		}

		entities = append(entities, e)
	}

	return entities, nil
}

func mapSingleRow[E any](mapper MapperFunc[E], rows *sql.Rows, err error) (E, error) {
	results, err := mapRows(mapper, rows, err)
	if err != nil {
		var empty E
		return empty, err
	}

	if len(results) == 0 {
		var empty E
		return empty, sql.ErrNoRows
	}

	return results[0], nil
}
