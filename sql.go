package sql

import (
	"context"
	"database/sql"

	"github.com/iden3/go-merkletree-sql/v2"
)

const schema = `
  CREATE TABLE IF NOT EXISTS mt_nodes (
    mt_id INTEGER,
    key BLOB,
    type INTEGER NOT NULL,
    child_l BLOB,
    child_r BLOB,
    entry BLOB,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY(mt_id, key)
  );

  CREATE TABLE IF NOT EXISTS mt_roots (
    mt_id INTEGER PRIMARY KEY,
    key BLOB,
    created_at INTEGER,
    deleted_at INTEGER
  );
`

const upsertStmt = `INSERT INTO mt_nodes (mt_id, key, type, child_l, child_r, entry) VALUES (?, ?, ?, ?, ?, ?) ` +
	`ON CONFLICT (mt_id, key) DO UPDATE SET type = ?, child_l = ?, child_r = ?, entry = ?`

const updateRootStmt = `INSERT INTO mt_roots (mt_id, key) VALUES (?, ?) ` +
	`ON CONFLICT (mt_id) DO UPDATE SET key = ?`

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// Storage implements the db.Storage interface
type Storage struct {
	db             DB
	mtId           uint64
	currentVersion uint64
	currentRoot    *merkletree.Hash
}

type NodeItem struct {
	MTId uint64 `db:"mt_id"`
	Key  []byte `db:"key"`
	// Type is the type of node in the tree.
	Type byte `db:"type"`
	// ChildL is the left child of a middle node.
	ChildL []byte `db:"child_l"`
	// ChildR is the right child of a middle node.
	ChildR []byte `db:"child_r"`
	// Entry is the data stored in a leaf node.
	Entry     []byte  `db:"entry"`
	CreatedAt *uint64 `db:"created_at"`
	DeletedAt *uint64 `db:"deleted_at"`
}

type RootItem struct {
	MTId      uint64  `db:"mt_id"`
	Key       []byte  `db:"key"`
	CreatedAt *uint64 `db:"created_at"`
	DeletedAt *uint64 `db:"deleted_at"`
}

// NewSqlStorage returns a new Storage
func NewSqlStorage(db DB, mtId uint64) *Storage {
	return &Storage{db: db, mtId: mtId}
}

// Get retrieves a value from a key in the db.Storage
func (s *Storage) Get(ctx context.Context,
	key []byte) (*merkletree.Node, error) {
	item := NodeItem{}
	err := s.db.GetContext(ctx, &item,
		"SELECT * FROM mt_nodes WHERE mt_id = ? AND key = ?", s.mtId, key)
	if err == sql.ErrNoRows {
		return nil, merkletree.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	node, err := item.Node()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (s *Storage) Put(ctx context.Context, key []byte,
	node *merkletree.Node) error {

	var childL []byte
	if node.ChildL != nil {
		childL = append(childL, node.ChildL[:]...)
	}

	var childR []byte
	if node.ChildR != nil {
		childR = append(childR, node.ChildR[:]...)
	}

	var entry []byte
	if node.Entry[0] != nil && node.Entry[1] != nil {
		entry = append(node.Entry[0][:], node.Entry[1][:]...)
	}

	_, err := s.db.ExecContext(ctx, upsertStmt, s.mtId, key[:], node.Type,
		childL, childR, entry, node.Type, childL, childR, entry)
	return err
}

// GetRoot retrieves a merkle tree root hash in the interface db.Tx
func (s *Storage) GetRoot(ctx context.Context) (*merkletree.Hash, error) {
	var root merkletree.Hash
	var err error

	if s.currentRoot != nil {
		copy(root[:], s.currentRoot[:])
		return &root, nil
	}

	item := RootItem{}
	err = s.db.GetContext(ctx, &item,
		"SELECT * FROM mt_roots WHERE mt_id = ?", s.mtId)
	if err == sql.ErrNoRows {
		return nil, merkletree.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if s.currentRoot == nil {
		s.currentRoot = &merkletree.Hash{}
	}
	copy(s.currentRoot[:], item.Key[:])
	copy(root[:], s.currentRoot[:])
	return &root, nil
}

func (s *Storage) SetRoot(ctx context.Context, hash *merkletree.Hash) error {
	if s.currentRoot == nil {
		s.currentRoot = &merkletree.Hash{}
	}
	copy(s.currentRoot[:], hash[:])
	_, err := s.db.ExecContext(ctx, updateRootStmt, s.mtId, s.currentRoot[:], s.currentRoot[:])
	if err != nil {
		err = newErr(err, "failed to update current root hash")
	}
	return err
}

func (item *NodeItem) Node() (*merkletree.Node, error) {
	node := merkletree.Node{
		Type: merkletree.NodeType(item.Type),
	}
	if item.ChildL != nil {
		node.ChildL = &merkletree.Hash{}
		copy(node.ChildL[:], item.ChildL[:])
	}
	if item.ChildR != nil {
		node.ChildR = &merkletree.Hash{}
		copy(node.ChildR[:], item.ChildR[:])
	}
	if len(item.Entry) > 0 {
		if len(item.Entry) != 2*merkletree.ElemBytesLen {
			return nil, merkletree.ErrNodeBytesBadSize
		}
		node.Entry = [2]*merkletree.Hash{{}, {}}
		copy(node.Entry[0][:], item.Entry[0:32])
		copy(node.Entry[1][:], item.Entry[32:64])
	}
	return &node, nil
}

// KV contains a key (K) and a value (V)
type KV struct {
	MTId uint64
	K    []byte
	V    merkletree.Node
}

type storageError struct {
	err error
	msg string
}

func (err storageError) Error() string {
	return err.msg + ": " + err.err.Error()
}

func (err storageError) Unwrap() error {
	return err.err
}

func newErr(err error, msg string) error {
	return storageError{err, msg}
}
