package sql

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/iden3/go-merkletree-sql/v2"
	"github.com/iden3/go-merkletree-sql/v2/db/test"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

var maxMTId uint64 = 0

type SqlStorageBuilder struct{}

func (builder *SqlStorageBuilder) NewStorage(t *testing.T) merkletree.Storage {
	db, err := sqlx.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec(schema)
	require.NoError(t, err)

	mtId := atomic.AddUint64(&maxMTId, 1)

	return NewSqlStorage(db, mtId)
}

func TestSql(t *testing.T) {
	builder := &SqlStorageBuilder{}
	test.TestAll(t, builder)
}

func TestErrors(t *testing.T) {
	err := storageError{
		err: io.EOF,
		msg: "storage error",
	}
	require.EqualError(t, err, "storage error: EOF")
	require.Equal(t, io.EOF, errors.Unwrap(err))
}
