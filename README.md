# Rowmap

A Go library to map SQL query results to Go structs.

# Basic usage

Install

```
go get github.com/spearson78/rowmap
```

Define a struct and a mapper from an sql.Rows to your struct

```go
type TestStruct struct {
	Id  int64
	Col string
}

func testStructMapper(row *sql.Rows) (TestStruct, error) {
	var e TestStruct
	err := row.Scan(&e.Id, &e.Col)
	return e, err
}
```

Execute queries

```go
entities, err := Query(db,testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
```
