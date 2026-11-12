package item

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotFound is returned when no item has the requested id.
var ErrNotFound = errors.New("item not found")

// Store reads and writes items in MySQL.
type Store struct {
	db *sql.DB
}

// NewStore returns a Store that uses the given connection pool.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// queryer is satisfied by both *sql.DB and *sql.Tx, so get works inside or
// outside a transaction.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// get reads one item by id. It returns an error wrapping ErrNotFound if no row has that id.
func get(ctx context.Context, q queryer, id int64) (*Item, error) {
	var it Item
	row := q.QueryRowContext(ctx, `
		SELECT id, name, description, price_cents, currency, stock, created_at, updated_at
		FROM items
		WHERE id = ?`, id)
	err := row.Scan(
		&it.ID,
		&it.Name,
		&it.Description,
		&it.PriceCents,
		&it.Currency,
		&it.Stock,
		&it.CreatedAt,
		&it.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: id %d", ErrNotFound, id)
		}
		return nil, fmt.Errorf("select item %d: %w", id, err)
	}
	return &it, nil
}

// Get reads item id. It returns an error wrapping ErrNotFound if no item has
// that id.
func (s *Store) Get(ctx context.Context, id int64) (*Item, error) {
	return get(ctx, s.db, id)
}

// writeAndRead runs write in a transaction, reads back the item whose id
// write returns, and commits. Create and Update differ only in write.
func (s *Store) writeAndRead(ctx context.Context, write func(tx *sql.Tx) (int64, error)) (*Item, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	// Undoes the write on any early return; a no-op after Commit succeeds.
	defer func() { _ = tx.Rollback() }()

	id, err := write(tx)
	if err != nil {
		return nil, err
	}

	// Read back through tx: only it can see the uncommitted change, and the
	// response then carries the timestamps MySQL actually stored.
	it, err := get(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return it, nil
}

// Create inserts in and returns the stored item. The caller must have validated in.
func (s *Store) Create(ctx context.Context, in *Input) (*Item, error) {
	return s.writeAndRead(ctx, func(tx *sql.Tx) (int64, error) {
		// nil Description/Stock pointers are written as NULL.
		res, err := tx.ExecContext(ctx, `
			INSERT INTO items (name, description, price_cents, currency, stock)
			VALUES (?, ?, ?, ?, ?)`,
			in.Name, in.Description, *in.PriceCents, in.Currency, in.Stock,
		)
		if err != nil {
			return 0, fmt.Errorf("insert item: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("last insert id: %w", err)
		}
		return id, nil
	})
}

// Update replaces every field of item id with in and returns the stored item.
// It returns an error wrapping ErrNotFound if no item has that id.
// The caller must have validated in.
func (s *Store) Update(ctx context.Context, id int64, in *Input) (*Item, error) {
	return s.writeAndRead(ctx, func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx, `
			UPDATE items
			SET name = ?, description = ?, price_cents = ?, currency = ?, stock = ?
			WHERE id = ?`,
			in.Name, in.Description, *in.PriceCents, in.Currency, in.Stock, id,
		)
		if err != nil {
			return 0, fmt.Errorf("update item: %w", err)
		}
		// Matched rows, not changed rows, thanks to ClientFoundRows.
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("rows affected: %w", err)
		}
		if n == 0 {
			return 0, fmt.Errorf("%w: id %d", ErrNotFound, id)
		}
		return id, nil
	})
}

// Delete removes item id. It returns an error wrapping ErrNotFound if no item
// has that id.
func (s *Store) Delete(ctx context.Context, id int64) error {
	// One statement is atomic by itself and there is nothing to read back,
	// so this skips writeAndRead and its transaction.
	res, err := s.db.ExecContext(ctx, `DELETE FROM items WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete item: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: id %d", ErrNotFound, id)
	}
	return nil
}
