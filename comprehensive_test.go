package dalgo2databricks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type userStruct struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Score float64
}

func TestComprehensive_ClientCloseAndMethods(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	b := &backend{client: c}
	assert.Equal(t, "main", b.ID())
	assert.Equal(t, "dalgo2databricks", b.Adapter().Name())
	assert.Nil(t, b.Schema())

	// Test Close
	err = c.Close()
	assert.NoError(t, err)

	// Calling begin after close should fail
	_, err = c.begin(context.Background())
	assert.Error(t, err)
}

func TestComprehensive_MapTarget(t *testing.T) {
	strVal := "alice"
	numVal := "42"
	fltVal := "99.5"

	row := Row{
		"id":    &strVal,
		"name":  &strVal,
		"age":   &numVal,
		"Score": &fltVal,
	}

	// Map to struct pointer
	var u userStruct
	err := mapTarget(&u, row)
	require.NoError(t, err)
	assert.Equal(t, "alice", u.ID)
	assert.Equal(t, "alice", u.Name)
	assert.Equal(t, 42, u.Age)
	assert.Equal(t, 99.5, u.Score)

	// Map to map[string]any
	m := make(map[string]any)
	err = mapTarget(&m, row)
	require.NoError(t, err)
	assert.Equal(t, "alice", m["name"])

	// Map to unsupported non-pointer target
	var nonPtr int
	err = mapTarget(nonPtr, row)
	assert.Error(t, err)
}

func TestComprehensive_TypeMappings(t *testing.T) {
	cases := []struct {
		dbType   string
		expected dbschema.Type
	}{
		{"BOOLEAN", dbschema.Bool},
		{"TINYINT", dbschema.Int},
		{"SMALLINT", dbschema.Int},
		{"INT", dbschema.Int},
		{"INTEGER", dbschema.Int},
		{"BIGINT", dbschema.Int},
		{"FLOAT", dbschema.Float},
		{"REAL", dbschema.Float},
		{"DOUBLE", dbschema.Float},
		{"DECIMAL(10,2)", dbschema.Decimal},
		{"DATE", dbschema.Time},
		{"TIMESTAMP", dbschema.Time},
		{"TIMESTAMP_NTZ", dbschema.Time},
		{"BINARY", dbschema.Bytes},
		{"BYTE", dbschema.Int},
		{"ARRAY<STRING>", dbschema.String},
		{"MAP<STRING, INT>", dbschema.String},
		{"STRUCT<x: INT>", dbschema.String},
		{"JSON", dbschema.String},
		{"UNKNOWN_TYPE", dbschema.String},
	}

	for _, tc := range cases {
		got := mapDatabricksType(tc.dbType)
		assert.Equal(t, tc.expected, got, "type: %s", tc.dbType)
	}
}

func TestComprehensive_BackendSchemaReaderDelegation(t *testing.T) {
	ts := mockMetadataServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	b := &backend{client: c}
	ctx := context.Background()

	// ListCollections
	colls, err := b.ListCollections(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, colls, 2)

	// DescribeCollection
	ref := dal.NewRootCollectionRef("customers", "")
	def, err := b.DescribeCollection(ctx, &ref)
	require.NoError(t, err)
	assert.Equal(t, "customers", def.Name)

	// ListIndexes
	indexes, err := b.ListIndexes(ctx, &ref)
	require.NoError(t, err)
	assert.Empty(t, indexes)

	// ListConstraints
	constraints, err := b.ListConstraints(ctx, &ref)
	require.NoError(t, err)
	assert.Len(t, constraints, 1)

	// ListReferrers
	referrers, err := b.ListReferrers(ctx, &ref)
	require.NoError(t, err)
	assert.Len(t, referrers, 1)
}

func TestComprehensive_StatementFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := statementResponse{
			StatementID: "stmt-fail",
			Status: statementStatus{
				State: "FAILED",
				Error: &statementError{
					Message:   "TABLE_OR_VIEW_NOT_FOUND: table not found",
					ErrorCode: "TABLE_OR_VIEW_NOT_FOUND",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	q := dal.From(dal.NewRootCollectionRef("missing", "")).NewQuery().SelectIntoRecordset()
	_, err = c.Query(context.Background(), q)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execution")
}

func TestComprehensive_StatementCanceled(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := statementResponse{
			StatementID: "stmt-cancel",
			Status: statementStatus{
				State: "CANCELED",
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectIntoRecordset()
	_, err = c.Query(context.Background(), q)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "canceled")
}

func TestComprehensive_QueryWithGroupByAndHaving(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("", "status"), dal.Equal, dal.NewConstant("completed"))).
		GroupBy(dal.NewFieldRef("", "customer_id")).
		Having(dal.NewComparison(dal.NewFieldRef("", "total"), dal.GreaterThen, dal.NewConstant(100))).
		OrderBy(dal.Descending(dal.NewFieldRef("", "customer_id"))).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("", "customer_id")},
		)

	p, err := c.compile(o, q, false)
	require.NoError(t, err)

	expectedSQL := "SELECT `customer_id` FROM `main`.`analytics`.`orders` WHERE `status` = :p0 GROUP BY `customer_id` HAVING `total` > :p1 ORDER BY `customer_id` DESC"
	assert.Equal(t, expectedSQL, p.sql)
	require.Len(t, p.parameters, 2)
	assert.Equal(t, "completed", p.parameters[0].Value)
	assert.Equal(t, "100", p.parameters[1].Value)
}

func TestComprehensive_DynamicTokenProvider(t *testing.T) {
	callCount := 0
	provider := func(ctx context.Context) (Credential, error) {
		callCount++
		return BearerToken("dyn-token-123"), nil
	}

	opts := validTestOptions()
	opts.Token = provider
	c, err := New("https://adb-1234.azuredatabricks.net", opts)
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	cred, err := o.auth()
	require.NoError(t, err)
	assert.Equal(t, "dyn-token-123", cred.raw)
	assert.Equal(t, 1, callCount)
}

func TestComprehensive_TimeoutConfiguration(t *testing.T) {
	opts := validTestOptions()
	opts.Timeout = 100 * time.Millisecond
	opts.PollInterval = 10 * time.Millisecond

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c, err := New(ts.URL, opts)
	require.NoError(t, err)

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectIntoRecordset()
	_, err = c.Query(context.Background(), q)
	assert.Error(t, err)
}
