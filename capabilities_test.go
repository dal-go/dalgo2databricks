package dalgo2databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilities(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	caps := c.QueryCapabilities()
	assert.True(t, caps.GroupBy)
	assert.True(t, caps.Having)
	assert.True(t, caps.OrderBy)
	assert.True(t, caps.Aggregate.Count)
	assert.True(t, caps.Aggregate.Sum)
	assert.True(t, caps.Aggregate.Avg)
	assert.True(t, caps.Aggregate.Min)
	assert.True(t, caps.Aggregate.Max)

	db := c.DB()
	backendCaps, ok := db.(interface {
		QueryCapabilities() dalgoQueryCapabilities
	})
	_ = backendCaps
	_ = ok
}

type dalgoQueryCapabilities interface{}
