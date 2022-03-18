package rowmap

import (
	"context"
	"database/sql"
	"testing"
)

func TestStmtQuery(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entities, err := stmt.Query(1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestStmtQueryTx(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	tx, err := db.Begin()

	stmt, err := Prepare(tx, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entities, err := stmt.Query(1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestStmtQueryConn(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	conn, err := db.Conn(context.Background())

	stmt, err := Prepare(conn, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entities, err := stmt.Query(1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestStmtQueryNoRows(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IS NULL ORDER BY ID")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entities, err := stmt.Query()
	if err != nil {
		t.Fatalf("query failed  %v", err)
	}

	//Preferred go style is nil for empty slices
	if entities != nil {
		t.Fatal("entities == nil")
	}
}

func TestStmtQueryContextSuccess(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	ctx := context.Background()

	stmt, err := PrepareContext(ctx, db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entities, err := stmt.QueryContext(ctx, 1, 2)
	checkBasicQueryResults(t, entities, err)
}

func TestStmtQueryContextPrepareCancelled(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = PrepareContext(ctx, db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err == nil {
		t.Fatal("prepare succeded with cancelled context")
	}
}

func TestStmtQueryContextQueryCancelled(t *testing.T) {

	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	stmt, err := PrepareContext(ctx, db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	cancel()
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	_, err = stmt.QueryContext(ctx, 1, 2)
	if err == nil {
		t.Fatal("query succeded with cancelled context")
	}
}

func TestStmtQueryRow(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entity, err := stmt.QueryRow(1, 2)
	checkBasicQueryRowResult(t, entity, err)
}

func TestStmtQueryRowNoRows(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IS NULL ORDER BY ID")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	_, err = stmt.QueryRow()
	if err != sql.ErrNoRows {
		t.Fatal("err != sql.ErrNoRows")
	}
}

func TestStmtQueryRowContext(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST WHERE ID IN (?,?) ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	entity, err := stmt.QueryRowContext(context.Background(), 1, 2)
	checkBasicQueryRowResult(t, entity, err)
}

func TestStmtQueryRowContextCancelled(t *testing.T) {
	db, err := newTestDb()
	if err != nil {
		t.Fatalf("newTestDb failed  %v", err)
		return
	}

	stmt, err := Prepare(db, testStructMapper, "SELECT ID,COL FROM TEST ORDER BY ID DESC")
	if err != nil {
		t.Fatalf("Prepare failed  %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = stmt.QueryRowContext(ctx)
	if err == nil {
		t.Fatal("query succeded with cancelled context")
	}

}
