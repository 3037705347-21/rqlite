package db

import (
	"errors"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func keyedRequest(key string, sqls ...string) *command.Request {
	stmts := make([]*command.Statement, len(sqls))
	for i := range sqls {
		stmts[i] = &command.Statement{Sql: sqls[i]}
	}
	return &command.Request{
		IdempotencyKey: key,
		Statements:     stmts,
	}
}

// Test_DB_ExecuteIdempotencyKey_Dedup verifies that replaying an identical
// keyed request skips the user statements and returns the duplicated marker.
func Test_DB_ExecuteIdempotencyKey_Dedup(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()

	mustExecute(db, "CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
	mustExecute(db, "INSERT INTO acct VALUES(1, 0)")

	req := keyedRequest("txn-1", "UPDATE acct SET n=n+1 WHERE id=1")
	res, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("first keyed execute failed: %s", err.Error())
	}
	if len(res) != 1 || res[0].GetDuplicated() {
		t.Fatalf("unexpected first response: %s", asJSON(res))
	}

	// Identical replay must not apply the statements a second time.
	res, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("identical replay failed: %s", err.Error())
	}
	if len(res) != 1 || !res[0].GetDuplicated() {
		t.Fatalf("expected a single duplicated response, got: %s", asJSON(res))
	}

	q, err := db.QueryStringStmt("SELECT n FROM acct WHERE id=1")
	if err != nil {
		t.Fatalf("failed to query balance: %s", err.Error())
	}
	if exp, got := `[{"columns":["n"],"types":["integer"],"values":[[1]]}]`, asJSON(q); exp != got {
		t.Fatalf("expected exactly one decrement, expected %s, got %s", exp, got)
	}

	q, err = db.QueryStringStmt("SELECT COUNT(*) FROM " + IdempotencyTable)
	if err != nil {
		t.Fatalf("failed to query internal table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`, asJSON(q); exp != got {
		t.Fatalf("expected one dedup record, expected %s, got %s", exp, got)
	}
}

// Test_DB_ExecuteIdempotencyKey_Conflict verifies that reusing a key with a
// different payload is an ordinary conflict error, never a FatalError, and
// leaves the database unchanged.
func Test_DB_ExecuteIdempotencyKey_Conflict(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()

	mustExecute(db, "CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
	mustExecute(db, "INSERT INTO acct VALUES(1, 0)")

	if _, err := db.Execute(keyedRequest("txn-2", "UPDATE acct SET n=n+1 WHERE id=1"), false); err != nil {
		t.Fatalf("first keyed execute failed: %s", err.Error())
	}

	_, err := db.Execute(keyedRequest("txn-2", "UPDATE acct SET n=n+50 WHERE id=1"), false)
	if !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("expected ErrIdempotencyKeyConflict, got %v", err)
	}
	if isFatalError(err) {
		t.Fatalf("conflict must not be classified as a FatalError, got %T", err)
	}

	q, err := db.QueryStringStmt("SELECT n FROM acct WHERE id=1")
	if err != nil {
		t.Fatalf("failed to query balance: %s", err.Error())
	}
	if exp, got := `[{"columns":["n"],"types":["integer"],"values":[[1]]}]`, asJSON(q); exp != got {
		t.Fatalf("conflict must not mutate data, expected %s, got %s", exp, got)
	}
}

// Test_DB_ExecuteIdempotencyKey_AtomicRollback verifies that a failing
// statement inside a keyed request rolls back both the user changes and the
// dedup record, allowing the same key to be retried successfully.
func Test_DB_ExecuteIdempotencyKey_AtomicRollback(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()

	mustExecute(db, "CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
	mustExecute(db, "INSERT INTO acct VALUES(1, 0)")

	// Commit one keyed request first so the internal table exists; its DDL
	// is itself transactional and would otherwise be absent after rollback.
	if _, err := db.Execute(keyedRequest("seed", "UPDATE acct SET n=n WHERE id=1"), false); err != nil {
		t.Fatalf("seed keyed execute failed: %s", err.Error())
	}

	bad := keyedRequest("txn-3",
		"UPDATE acct SET n=n+1 WHERE id=1",
		"INSERT INTO acct(id, n) VALUES(1, 9)", // PRIMARY KEY conflict
	)
	// A SQL failure inside the forced transaction rolls the whole request
	// back; rqlite carries such errors in the per-statement results rather
	// than as a Go error.
	res, err := db.Execute(bad, false)
	if err != nil {
		t.Fatalf("SQL failures are carried in results, got Go error: %s", err.Error())
	}
	failed := false
	for _, r := range res {
		if r.GetE().GetError() != "" {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("expected a per-statement SQL error, got: %s", asJSON(res))
	}

	q, err := db.QueryStringStmt("SELECT n FROM acct WHERE id=1")
	if err != nil {
		t.Fatalf("failed to query balance: %s", err.Error())
	}
	if exp, got := `[{"columns":["n"],"types":["integer"],"values":[[0]]}]`, asJSON(q); exp != got {
		t.Fatalf("rollback must undo partial writes, expected %s, got %s", exp, got)
	}

	q, err = db.QueryStringStmt("SELECT COUNT(*) FROM " + IdempotencyTable + " WHERE id='txn-3'")
	if err != nil {
		t.Fatalf("failed to query dedup record: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[0]]}]`, asJSON(q); exp != got {
		t.Fatalf("rollback must remove the dedup record, expected %s, got %s", exp, got)
	}

	// Same key, corrected payload -> applies exactly once.
	good := keyedRequest("txn-3", "UPDATE acct SET n=n+1 WHERE id=1")
	if _, err := db.Execute(good, false); err != nil {
		t.Fatalf("retried keyed execute failed: %s", err.Error())
	}
	q, err = db.QueryStringStmt("SELECT n FROM acct WHERE id=1")
	if err != nil {
		t.Fatalf("failed to query balance post-retry: %s", err.Error())
	}
	if exp, got := `[{"columns":["n"],"types":["integer"],"values":[[1]]}]`, asJSON(q); exp != got {
		t.Fatalf("retry expected to apply once, expected %s, got %s", exp, got)
	}
}

type fakeColumnsProvider struct{}

func (f *fakeColumnsProvider) ColumnNames(table string) ([]string, error) {
	return nil, nil
}

// Test_CDCCollator_IdempotencyTableFiltered verifies that changes to the
// internal dedup table never reach CDC consumers.
func Test_CDCCollator_IdempotencyTableFiltered(t *testing.T) {
	c, err := NewCDCCollator(&fakeColumnsProvider{})
	if err != nil {
		t.Fatalf("failed to create collator: %s", err.Error())
	}

	if err := c.PreupdateHook(&command.CDCEvent{Table: IdempotencyTable, NewRowId: 1}); err != nil {
		t.Fatalf("internal table event errored: %s", err.Error())
	}
	c.CommitHook()
	if events := c.Events(); len(events) != 0 {
		t.Fatalf("internal table events must be filtered, got %d: %s", len(events), asJSON(events))
	}

	// User-table events still pass through.
	c.Reset()
	if err := c.PreupdateHook(&command.CDCEvent{Table: "acct", NewRowId: 2}); err != nil {
		t.Fatalf("user table event errored: %s", err.Error())
	}
	c.CommitHook()
	if events := c.Events(); len(events) != 1 || events[0].Table != "acct" {
		t.Fatalf("user table event must be retained, got: %s", asJSON(c.Events()))
	}
}
