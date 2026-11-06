package dalgo2databricks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockDatabricksServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Verify authorization header
		auth := r.Header.Get("Authorization")
		if auth != "Bearer dapi_test_token_12345" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method == http.MethodPost && r.URL.Path == "/api/2.0/sql/statements" {
			var req statementRequest
			_ = json.NewDecoder(r.Body).Decode(&req)

			resp := statementResponse{
				StatementID: "stmt-001",
				Status: statementStatus{
					State: "SUCCEEDED",
				},
				Manifest: &statementManifest{
					Format: "JSON_ARRAY",
					Schema: manifestSchema{
						ColumnCount: 3,
						Columns: []manifestColumn{
							{Name: "id", TypeName: "STRING", Position: 0},
							{Name: "name", TypeName: "STRING", Position: 1},
							{Name: "score", TypeName: "DOUBLE", Position: 2},
						},
					},
					TotalRowCount: 1,
				},
				Result: &statementResult{
					ChunkIndex: 0,
					RowCount:   1,
					DataArray: [][]any{
						{"u-100", "Alice", "98.5"},
					},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
}

func TestReads_GetAndExists(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	opts := validTestOptions()
	opts.Timeout = 5 * time.Second
	c, err := New(ts.URL, opts)
	require.NoError(t, err)

	ctx := context.Background()

	// Test Get
	doc, err := c.Get(ctx, "users", "u-100")
	require.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, "u-100", doc.ID)
	assert.Equal(t, "Alice", *doc.Row["name"])
	assert.Equal(t, "98.5", *doc.Row["score"])

	// Test Exists
	exists, err := c.Exists(ctx, "users", "u-100")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestReads_GetMany(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	ctx := context.Background()
	docs, err := c.GetMany(ctx, "users", []string{"u-100"})
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, "u-100", docs[0].ID)
}

func TestReads_Query(t *testing.T) {
	ts := mockDatabricksServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	ctx := context.Background()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("", "id")},
			dal.Column{Expression: dal.NewFieldRef("", "name")},
		)

	res, err := c.Query(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "Alice", *res.Rows[0]["name"])
}

func TestReads_PollingState(t *testing.T) {
	pollCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			// First call returns PENDING
			resp := statementResponse{
				StatementID: "stmt-poll",
				Status: statementStatus{
					State: "PENDING",
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/2.0/sql/statements/stmt-poll" {
			pollCount++
			resp := statementResponse{
				StatementID: "stmt-poll",
				Status: statementStatus{
					State: "SUCCEEDED",
				},
				Manifest: &statementManifest{
					Format: "JSON_ARRAY",
					Schema: manifestSchema{
						ColumnCount: 2,
						Columns: []manifestColumn{
							{Name: "id", TypeName: "STRING"},
							{Name: "name", TypeName: "STRING"},
						},
					},
				},
				Result: &statementResult{
					DataArray: [][]any{{"u-100", "PolledUser"}},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
	}))
	defer ts.Close()

	opts := validTestOptions()
	opts.ImmediatePolling = true
	c, err := New(ts.URL, opts)
	require.NoError(t, err)

	doc, err := c.Get(context.Background(), "users", "u-100")
	require.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, "PolledUser", *doc.Row["name"])
	assert.GreaterOrEqual(t, pollCount, 1)
}

func TestReads_Pagination(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			nextChunk := 1
			resp := statementResponse{
				StatementID: "stmt-pages",
				Status: statementStatus{
					State: "SUCCEEDED",
				},
				Manifest: &statementManifest{
					Format: "JSON_ARRAY",
					Schema: manifestSchema{
						ColumnCount: 2,
						Columns: []manifestColumn{
							{Name: "id", TypeName: "STRING"},
							{Name: "name", TypeName: "STRING"},
						},
					},
				},
				Result: &statementResult{
					ChunkIndex:     0,
					DataArray:      [][]any{{"u-1", "User1"}},
					NextChunkIndex: &nextChunk,
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/2.0/sql/statements/stmt-pages/result/chunks/1" {
			resp := statementResult{
				ChunkIndex: 1,
				DataArray:  [][]any{{"u-2", "User2"}},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
	}))
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectIntoRecordset()
	res, err := c.Query(context.Background(), q)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	assert.Equal(t, "User1", *res.Rows[0]["name"])
	assert.Equal(t, "User2", *res.Rows[1]["name"])
}
