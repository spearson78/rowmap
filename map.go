package rowmap

import (
	"database/sql"

	"github.com/Southclaws/fault"
	"github.com/Southclaws/fault/floc"
)

func mapRows[E any](mapper MapperFunc[E], rows *sql.Rows, err error) ([]E, error) {
	if err != nil {
		return nil, fault.Wrap(err, With(mapper), floc.WithDepth(2))
	}
	defer rows.Close()

	var entities []E
	for rows.Next() {

		e, err := mapper(rows)
		if err != nil {
			return nil, fault.Wrap(err, With(mapper), floc.WithDepth(2))
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
		return empty, fault.Wrap(sql.ErrNoRows, With(mapper), floc.WithDepth(2))
	}

	return results[0], nil
}
