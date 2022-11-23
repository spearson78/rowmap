package rowmap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"
)

type TestStruct struct {
	Id  int64
	Col string
}

func newTestDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("CREATE TABLE TEST (ID TEXT PRIMARY KEY,COL TEXT)")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("INSERT INTO TEST (ID,COL) VALUES(?,?)", 1, "Row 1")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("INSERT INTO TEST (ID,COL) VALUES(?,?)", 2, "Row 2")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("INSERT INTO TEST (ID,COL) VALUES(?,?)", 3, "Row 3")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func testStructMapper(row *sql.Rows) (TestStruct, error) {
	var e TestStruct
	err := row.Scan(&e.Id, &e.Col)
	return e, err
}

func checkBasicQueryResults(t *testing.T, entities []TestStruct, err error) {
	if err != nil {
		t.Fatalf("Query failed  %v", err)
		return
	}

	if len(entities) != 2 {
		t.Fatal("len(entities) != 2")
		return
	}

	if entities[0].Id != 2 {
		t.Fatal("entities[0].Id != 2")
	}
	if entities[0].Col != "Row 2" {
		t.Fatal("entities[0].Col != \"Row 2\"")
	}

	if entities[1].Id != 1 {
		t.Fatal("entities[1].Id != 1")
	}
	if entities[1].Col != "Row 1" {
		t.Fatal("entities[1].Col != \"Row 1\"")
	}
}

func checkBasicQueryRowResult(t *testing.T, entity TestStruct, err error) {
	if err != nil {
		t.Fatalf("Query failed  %v", err)
		return
	}

	if entity.Id != 2 {
		t.Fatal("entities[0].Id != 2")
	}
	if entity.Col != "Row 2" {
		t.Fatal("entities[0].Col != \"Row 2\"")
	}
}

func TestQuery(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	entities, err := Query(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryResults(t, entities, err)
}

func ExampleQuery() {
	db, _ := newTestDb()
	entities, _ := Query(db, func(row *sql.Rows) (TestStruct, error) {
		var e TestStruct
		err := row.Scan(&e.Id, &e.Col)
		return e, err
	}, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	fmt.Println(entities)
	// Output: [{2 Row 2} {1 Row 1}]
}

func TestQueryTx(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	tx, err := db.Begin()

	entities, err := Query(tx, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestQueryConn(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	conn, err := db.Conn(context.Background())

	entities, err := Query(conn, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestQueryNoRows(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	entities, err := Query(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IS NULL ORDER BY ID")
	if err != nil {
		t.Fatalf("query failed  %v", err)
	}

	//Preferred go style is nil for empty slices
	if entities != nil {
		t.Fatal("entities == nil")
	}
}

func TestQueryContextSuccess(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	entities, err := QueryContext(context.Background(), db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryResults(t, entities, err)
}

func ExampleQueryContext() {
	db, _ := newTestDb()
	ctx := context.Background()
	entities, _ := QueryContext(ctx, db, func(row *sql.Rows) (TestStruct, error) {
		var e TestStruct
		err := row.Scan(&e.Id, &e.Col)
		return e, err
	}, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	fmt.Println(entities)
	// Output: [{2 Row 2} {1 Row 1}]
}

func TestQueryContextCancelled(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = QueryContext(ctx, db, testStructMapper, "SELECT ID,COL FROM TEST ORDER BY ID DESC")
	if err == nil {
		t.Fatal("query succeded with cancelled context")
	}
}

func TestQueryRow(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	entity, err := QueryRow(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryRowResult(t, entity, err)
}

func ExampleQueryRow() {
	db, _ := newTestDb()
	entity, _ := QueryRow(db, func(row *sql.Rows) (TestStruct, error) {
		var e TestStruct
		err := row.Scan(&e.Id, &e.Col)
		return e, err
	}, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	fmt.Println(entity)
	// Output: {2 Row 2}
}

func TestQueryRowNoRows(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	_, err = QueryRow(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IS NULL ORDER BY ID")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("err != sql.ErrNoRows")
	}
}

func TestQueryRowContext(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	entity, err := QueryRowContext(context.Background(), db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	checkBasicQueryRowResult(t, entity, err)
}

func ExampleQueryRowContext() {
	db, _ := newTestDb()
	ctx := context.Background()
	entity, _ := QueryRowContext(ctx, db, func(row *sql.Rows) (TestStruct, error) {
		var e TestStruct
		err := row.Scan(&e.Id, &e.Col)
		return e, err
	}, "SELECT ID,COL FROM TEST WHERE ID IN(?,?) ORDER BY ID DESC", 1, 2)
	fmt.Println(entity)
	// Output: {2 Row 2}
}

func TestQueryRowContextCancelled(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = QueryRowContext(ctx, db, testStructMapper, "SELECT ID,COL FROM TEST ORDER BY ID DESC")
	if err == nil {
		t.Fatal("query succeded with cancelled context")
	}
}
