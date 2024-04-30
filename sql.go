package sql

import (
	"context"
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

const upsertStmt = `INSERT INTO mt_nodes (mt_id, key, type, child_l, child_r, entry)
    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(mt_id, key) DO UPDATE SET
    type = excluded.type, child_l = excluded.child_l, child_r = excluded.child_r, entry = excluded.entry`

const updateRootStmt = `INSERT INTO mt_roots (mt_id, key) VALUES (?, ?)
    ON CONFLICT(mt_id) DO UPDATE SET key = excluded.key`

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type Storage struct {
	db   *sql.DB
	mtId uint64
}

type NodeItem struct {
	MTId   uint64
	Key    []byte
	Type   byte
	ChildL []byte
	ChildR []byte
	Entry  []byte
}

type RootItem struct {
	MTId uint64
	Key  []byte
}

func NewSqlStorage(db *sql.DB, mtId uint64) *Storage {
	return &Storage{db: db, mtId: mtId}
}

func (s *Storage) Get(ctx context.Context, key []byte) (*NodeItem, error) {
	item := NodeItem{}
	err := s.db.QueryRowContext(ctx,
		"SELECT mt_id, key, type, child_l, child_r, entry FROM mt_nodes WHERE mt_id = ? AND key = ?",
		s.mtId,
		key,
	).Scan(&item.MTId, &item.Key, &item.Type, &item.ChildL, &item.ChildR, &item.Entry)
	return &item, err
}
