package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB exposes the pgx methods used by Store.
type DB interface {
	Ping(ctx context.Context) error
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store wraps Postgres access for events, subscriptions, and deliveries.
type Store struct {
	db DB
}

// New creates a Store backed by the given connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{db: pool}
}

// NewFromDB creates a Store from an abstract DB connection.
func NewFromDB(db DB) *Store {
	return &Store{db: db}
}

// NewPool creates a pgx pool for the application store.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	return pool, nil
}

// Ping verifies the database connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.Ping(ctx)
}

// Ready verifies DB connectivity and a simple query for readiness checks.
func (s *Store) Ready(ctx context.Context) error {
	if err := s.db.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	var one int
	if err := s.db.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("readiness SELECT 1 failed: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("readiness query returned %d, want 1", one)
	}

	return nil
}
