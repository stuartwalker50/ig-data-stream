// Package store provides SQLite persistence for live price ticks.
// Each run creates a new database file named prices_YYYYMMDD_HHMMSS.db in the
// configured directory, making offline backup and backtesting straightforward.
package store

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

// Tick represents a single normalised price tick to be persisted.
type Tick struct {
	ReceivedAt time.Time
	Epic       string
	Bid        float64
	Ask        float64
	// Raw TIMESTAMP value from the Lightstreamer PRICE subscription (milliseconds
	// since epoch as published by IG).
	IGTimestampMs int64
	High          float64
	Low           float64
}

// Store manages a SQLite database file for price tick persistence.
type Store struct {
	db *sql.DB

	insertStmt *sql.Stmt
}

// Open creates (or opens) a timestamped SQLite database file in dir and
// initialises the prices table.
func Open(dir string) (*Store, error) {
	ts := time.Now().UTC().Format("20060102_150405")
	filename := filepath.Join(dir, fmt.Sprintf("prices_%s.db", ts))

	db, err := sql.Open("sqlite", filename)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", filename, err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS prices (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		received_at   TEXT    NOT NULL,
		epic          TEXT    NOT NULL,
		ig_ts_ms      INTEGER NOT NULL,
		bid           REAL    NOT NULL,
		ask           REAL    NOT NULL,
		high          REAL,
		low           REAL
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create prices table: %w", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_prices_epic ON prices(epic)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create index: %w", err)
	}

	stmt, err := db.Prepare(`INSERT INTO prices
		(received_at, epic, ig_ts_ms, bid, ask, high, low)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("prepare insert: %w", err)
	}

	slog.Info("opened price store", "file", filename)
	return &Store{db: db, insertStmt: stmt}, nil
}

// WriteTick persists a single price tick.
func (s *Store) WriteTick(t Tick) error {
	_, err := s.insertStmt.Exec(
		t.ReceivedAt.UTC().Format(time.RFC3339Nano),
		t.Epic,
		t.IGTimestampMs,
		t.Bid,
		t.Ask,
		nullableFloat(t.High),
		nullableFloat(t.Low),
	)
	if err != nil {
		return fmt.Errorf("write tick: %w", err)
	}
	return nil
}

// Close flushes and closes the database.
func (s *Store) Close() error {
	if s.insertStmt != nil {
		s.insertStmt.Close()
	}
	return s.db.Close()
}

// nullableFloat returns nil for zero values to allow NULL in the database.
func nullableFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
