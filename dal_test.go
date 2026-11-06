package dalgo2databricks

import (
	"context"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAL_BackendConformance(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	db := c.DB()
	require.NotNil(t, db)
	assert.Equal(t, "main", db.ID())
	assert.Equal(t, "dalgo2databricks", db.Adapter().Name())

	ctx := context.Background()

	// Transactions unsupported
	err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return nil
	})
	assert.Equal(t, dal.ErrNotSupported, err)

	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return nil
	})
	assert.Equal(t, dal.ErrNotSupported, err)
}

func TestDAL_GetRecord(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	db := c.DB()
	ctx := context.Background()

	key := record.NewKeyWithID("users", "u-100")
	userData := make(map[string]any)
	rec := record.NewRecordWithData(key, &userData)

	err = db.Get(ctx, rec)
	require.NoError(t, err)
	assert.Equal(t, "Alice", userData["name"])

	// Test Exists via DB
	b, ok := db.(interface {
		Exists(context.Context, *record.Key) (bool, error)
	})
	if ok {
		exists, err := b.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestDAL_RecordsReader(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	db := c.DB()
	ctx := context.Background()

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectIntoRecordset()
	reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	rec, err := reader.Next()
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "u-100", rec.Key().ID)

	// Cursor returns ErrNotSupported
	_, err = reader.Cursor()
	assert.Equal(t, dal.ErrNotSupported, err)

	// Next row reaches ErrNoMoreRecords
	_, err = reader.Next()
	assert.Equal(t, dal.ErrNoMoreRecords, err)
}

func TestDAL_RecordsetReader(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	db := c.DB()
	ctx := context.Background()

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectIntoRecordset()
	rsReader, err := db.ExecuteQueryToRecordsetReader(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, rsReader)
	defer rsReader.Close()

	assert.NotEmpty(t, rsReader.Recordset().Columns())

	row, _, err := rsReader.Next()
	require.NoError(t, err)
	require.NotNil(t, row)

	_, _, err = rsReader.Next()
	assert.Equal(t, dal.ErrNoMoreRecords, err)
}
