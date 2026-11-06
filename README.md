# dalgo2databricks

Production-quality Databricks adapter for the [DALgo unified data access framework](https://github.com/dal-go/dalgo), providing a unified data access and schema introspection engine for Databricks workspaces across AWS, Azure, and Google Cloud Platform (GCP).

[![Go Reference](https://pkg.go.dev/badge/github.com/dal-go/dalgo2databricks.svg)](https://pkg.go.dev/github.com/dal-go/dalgo2databricks)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Features

- **Multi-Cloud Support**: Single unified adapter for Databricks on AWS (`dbc-*.cloud.databricks.com`), Microsoft Azure (`adb-*.azuredatabricks.net`), and Google Cloud Platform (`*.gcp.databricks.com`), as well as custom private VPC endpoints.
- **Pure Go HTTP Transport**: Communicates with Databricks SQL Statement Execution REST API (`/api/2.0/sql/statements`). No CGO, Arrow Rust kernel, or Thrift bloat required.
- **Dalgo Integration**: Full conformance with DALgo `dal.Backend`, `dal.DB`, and `dbschema.SchemaReader`.
- **Query Execution**: Full support for parameterized queries, WHERE conditions, IN filters, NULL checks, GROUP BY, HAVING, and ORDER BY.
- **Result Streaming & Pagination**: Memory-bounded chunked retrieval of statement results via Databricks next-chunk links.
- **Safe Query Cancellation**: Asynchronous cancellation via `POST /api/2.0/sql/statements/{statement_id}/cancel` on context cancellation or query timeout.
- **Schema Discovery**: Automatic introspection of catalogs, schemas, tables, columns, data types, primary keys, and foreign keys via Unity Catalog `information_schema` with fallback to legacy metastores.
- **Credential Safety**: Strictly redacted credential types (`PersonalAccessToken`, `OAuthM2MToken`, `BearerToken`) that never leak tokens into logs, traces, or string formatters.
- **Dynamic Token Rotation**: Support for short-lived credentials via `TokenProvider` function (compatible with Azure Managed Identity, AWS IRSA, and GCP Service Account token rotation).

## Installation

```bash
go get github.com/dal-go/dalgo2databricks
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2databricks"
)

func main() {
	ctx := context.Background()

	// Initialize adapter
	client, err := dalgo2databricks.New("https://adb-1234567890123456.7.azuredatabricks.net", dalgo2databricks.Options{
		WarehouseID: "0123456789abcdef",
		Catalog:     "main",
		Schema:      "default",
		Token: func(ctx context.Context) (dalgo2databricks.Credential, error) {
			return dalgo2databricks.PersonalAccessToken("dapi..."), nil
		},
		Tables: map[string]dalgo2databricks.Table{
			"users": {Name: "users", IDColumn: "id"},
		},
	})
	if err != nil {
		log.Fatalf("failed to create databricks client: %v", err)
	}
	defer client.Close()

	// Obtain standard DALgo DB handle
	db := client.DB()

	// Execute structured query
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("", "status"), dal.Equal, dal.NewConstant("active"))).
		Limit(10).
		SelectIntoRecordset()

	reader, err := db.ExecuteQueryToRecordsetReader(ctx, q)
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	defer reader.Close()

	for {
		row, _, err := reader.Next()
		if err == dal.ErrNoMoreRecords {
			break
		}
		if err != nil {
			log.Fatalf("row read failed: %v", err)
		}
		fmt.Printf("Row: %v\n", row)
	}
}
```

## Authentication Options

### 1. Personal Access Token (PAT)
```go
opts.Token = func(ctx context.Context) (dalgo2databricks.Credential, error) {
    return dalgo2databricks.PersonalAccessToken("dapi123..."), nil
}
```

### 2. OAuth Machine-to-Machine (M2M)
```go
opts.Token = func(ctx context.Context) (dalgo2databricks.Credential, error) {
    return dalgo2databricks.OAuthM2MToken("token..."), nil
}
```

### 3. Dynamic Token Rotation (Cloud Identities)
For short-lived credentials obtained via AWS IAM roles, Azure Managed Identity, or Google Cloud Service Accounts:
```go
opts.Token = func(ctx context.Context) (dalgo2databricks.Credential, error) {
    token, err := cloudTokenSource.GetToken(ctx)
    if err != nil {
        return dalgo2databricks.Credential{}, err
    }
    return dalgo2databricks.BearerToken(token), nil
}
```

## Schema Introspection

The client implements `dbschema.SchemaReader`:

- `ListCollections(ctx, parent)`: Lists tables in current catalog/schema.
- `DescribeCollection(ctx, ref)`: Returns column types, nullability, and primary key constraints.
- `ListIndexes(ctx, ref)`: Returns empty slice (Delta Lake does not support secondary B-tree indexes).
- `ListConstraints(ctx, ref)`: Discovers primary key, foreign key, and check constraints.
- `ListReferrers(ctx, ref)`: Discovers foreign key dependencies pointing to the specified table.

## Read-Only Safety and Perimeter Controls

This adapter defaults to read-only query execution. Modifying statements (INSERT, UPDATE, DELETE) are not exposed by the primary DAL reader interface. When running within DataTug, DataTug Perimeter access boundary checks provide upstream authorization verification before queries reach the adapter.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
