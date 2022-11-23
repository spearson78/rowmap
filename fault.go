package rowmap

import (
	"errors"
	"fmt"
)

type withRowMap struct {
	wrapped    error
	entityType string
}

func (e *withRowMap) Error() string  { return fmt.Sprintf("<rowmap> : %v", e.wrapped) }
func (e *withRowMap) Cause() error   { return e.wrapped }
func (e *withRowMap) Unwrap() error  { return e.wrapped }
func (e *withRowMap) String() string { return e.Error() }

func Wrap[E any](err error, mapper MapperFunc[E]) error {
	if err == nil {
		return nil
	}

	var d E

	return &withRowMap{
		wrapped:    err,
		entityType: fmt.Sprintf("%T", d),
	}
}

func With[E any](sql string, mapper MapperFunc[E]) func(error) error {
	return func(err error) error {
		return Wrap(err, mapper)
	}
}

func Get(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var with *withRowMap
	if errors.As(err, &with) {
		return with.entityType, true
	}

	return "", false
}
