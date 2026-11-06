package dalgo2databricks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockMetadataServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/2.0/sql/statements" {
			var req statementRequest
			_ = json.NewDecoder(r.Body).Decode(&req)

			stmt := req.Statement
			if strings.Contains(stmt, "information_schema.tables") {
				resp := statementResponse{
					StatementID: "stmt-tables",
					Status:      statementStatus{State: "SUCCEEDED"},
					Manifest: &statementManifest{
						Schema: manifestSchema{
							Columns: []manifestColumn{{Name: "table_name", TypeName: "STRING"}},
						},
					},
					Result: &statementResult{
						DataArray: [][]any{{"customers"}, {"orders"}},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}

			if strings.Contains(stmt, "information_schema.columns") {
				resp := statementResponse{
					StatementID: "stmt-cols",
					Status:      statementStatus{State: "SUCCEEDED"},
					Manifest: &statementManifest{
						Schema: manifestSchema{
							Columns: []manifestColumn{
								{Name: "column_name", TypeName: "STRING"},
								{Name: "data_type", TypeName: "STRING"},
								{Name: "is_nullable", TypeName: "STRING"},
								{Name: "ordinal_position", TypeName: "INT"},
							},
						},
					},
					Result: &statementResult{
						DataArray: [][]any{
							{"id", "STRING", "NO", "1"},
							{"name", "STRING", "YES", "2"},
							{"balance", "DECIMAL(18,2)", "YES", "3"},
							{"active", "BOOLEAN", "NO", "4"},
						},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}

			if strings.Contains(stmt, "information_schema.referential_constraints") {
				resp := statementResponse{
					StatementID: "stmt-refs",
					Status:      statementStatus{State: "SUCCEEDED"},
					Manifest: &statementManifest{
						Schema: manifestSchema{
							Columns: []manifestColumn{
								{Name: "table_name", TypeName: "STRING"},
								{Name: "column_name", TypeName: "STRING"},
							},
						},
					},
					Result: &statementResult{
						DataArray: [][]any{
							{"orders", "customer_id"},
						},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}

			if strings.Contains(stmt, "information_schema.table_constraints") {
				resp := statementResponse{
					StatementID: "stmt-constraints",
					Status:      statementStatus{State: "SUCCEEDED"},
					Manifest: &statementManifest{
						Schema: manifestSchema{
							Columns: []manifestColumn{
								{Name: "constraint_name", TypeName: "STRING"},
								{Name: "constraint_type", TypeName: "STRING"},
								{Name: "column_name", TypeName: "STRING"},
							},
						},
					},
					Result: &statementResult{
						DataArray: [][]any{
							{"pk_customers", "PRIMARY KEY", "id"},
						},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}

			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

func TestSchemaReader_FullLifecycle(t *testing.T) {
	ts := mockMetadataServer(t)
	defer ts.Close()

	c, err := New(ts.URL, validTestOptions())
	require.NoError(t, err)

	ctx := context.Background()

	// 1. ListCollections
	colls, err := c.ListCollections(ctx, nil)
	require.NoError(t, err)
	require.Len(t, colls, 2)
	assert.Equal(t, "customers", colls[0].Name())
	assert.Equal(t, "orders", colls[1].Name())

	// 2. DescribeCollection
	ref := dal.NewRootCollectionRef("customers", "")
	def, err := c.DescribeCollection(ctx, &ref)
	require.NoError(t, err)
	require.NotNil(t, def)
	assert.Equal(t, "customers", def.Name)
	require.Len(t, def.Fields, 4)
	assert.Equal(t, dal.FieldName("id"), def.Fields[0].Name)
	assert.False(t, def.Fields[0].Nullable)
	assert.Equal(t, dbschema.String, def.Fields[0].Type)

	assert.Equal(t, dal.FieldName("balance"), def.Fields[2].Name)
	assert.Equal(t, dbschema.Decimal, def.Fields[2].Type)

	assert.Equal(t, dal.FieldName("active"), def.Fields[3].Name)
	assert.Equal(t, dbschema.Bool, def.Fields[3].Type)

	require.Len(t, def.PrimaryKey, 1)
	assert.Equal(t, dal.FieldName("id"), def.PrimaryKey[0])

	// 3. ListIndexes returns empty slice without error
	indexes, err := c.ListIndexes(ctx, &ref)
	require.NoError(t, err)
	assert.Empty(t, indexes)

	// 4. ListConstraints
	constraints, err := c.ListConstraints(ctx, &ref)
	require.NoError(t, err)
	require.Len(t, constraints, 1)
	assert.Equal(t, "primary-key", constraints[0].Type)

	// 5. ListReferrers
	referrers, err := c.ListReferrers(ctx, &ref)
	require.NoError(t, err)
	require.Len(t, referrers, 1)
	assert.Equal(t, "orders", referrers[0].Collection.Name())
	assert.Equal(t, []dal.FieldName{dal.FieldName("customer_id")}, referrers[0].Fields)
}
