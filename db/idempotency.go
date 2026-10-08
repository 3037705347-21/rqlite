package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"google.golang.org/protobuf/proto"
)

// IdempotencyTable is the reserved internal table in which the FSM records
// queued-write requests that have already been applied. The table is
// replicated through Raft and included in snapshots, so every node can
// deduplicate replayed requests. User schemas must not create a table with
// this name, and changes to the table are filtered out of CDC events.
const IdempotencyTable = "rqlite_internal_queued_requests"

const (
	createIdempotencyTableStmt = "CREATE TABLE IF NOT EXISTS " + IdempotencyTable +
		" (id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, applied_at INTEGER NOT NULL) WITHOUT ROWID"
	selectIdempotencyHashStmt = "SELECT payload_hash FROM " + IdempotencyTable + " WHERE id = ?"
	insertIdempotencyStmt     = "INSERT INTO " + IdempotencyTable + " (id, payload_hash, applied_at) VALUES (?, ?, ?)"
)

// ErrIdempotencyKeyConflict is returned as an ordinary (non-fatal) response
// error when a request carries an idempotency key that has already been
// recorded for a request with a different payload. It must not abort the FSM,
// because it is a deterministic, request-scoped condition.
var ErrIdempotencyKeyConflict = errors.New("idempotency key reused with different request content")

// idempotencyFingerprint returns a stable hex-encoded SHA-256 fingerprint of
// a Request. proto.Marshal is deterministic for this map-free message, so the
// fingerprint is identical on every Raft node and across retries.
func idempotencyFingerprint(req *command.Request) (string, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// lookupOrInsertIdempotencyKey enforces exactly-once execution for a keyed
// Request, inside the caller-supplied transaction.
//
// It returns duplicated=true when the same key has already been applied with
// an identical payload, in which case the caller must skip the user
// statements. A key reused with a different payload yields
// ErrIdempotencyKeyConflict and the caller rolls the transaction back. A new
// key gets its dedup record inserted, so it commits atomically together with
// the user statements.
func (db *DB) lookupOrInsertIdempotencyKey(ctx context.Context, eq execerQueryer, req *command.Request) (duplicated bool, retErr error) {
	if _, err := eq.ExecContext(ctx, createIdempotencyTableStmt); err != nil {
		return false, err
	}
	fingerprint, err := idempotencyFingerprint(req)
	if err != nil {
		return false, err
	}

	var existing string
	switch err := eq.QueryRowContext(ctx, selectIdempotencyHashStmt, req.IdempotencyKey).Scan(&existing); {
	case err == nil:
		if existing != fingerprint {
			return false, ErrIdempotencyKeyConflict
		}
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		// New key; fall through to insert.
	default:
		return false, err
	}

	if _, err := eq.ExecContext(ctx, insertIdempotencyStmt,
		req.IdempotencyKey, fingerprint, time.Now().UnixNano()); err != nil {
		return false, err
	}
	return false, nil
}
