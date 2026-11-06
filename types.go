// Package dalgo2databricks provides bounded read-only access to Databricks SQL
// warehouses and Unity Catalog across Microsoft Azure, Amazon Web Services (AWS),
// and Google Cloud Platform (GCP).
package dalgo2databricks

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Error deliberately has no underlying cause, URL, SQL, token or server body.
type Error struct {
	Code   string
	Status int
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("databricks: %s (HTTP %d)", e.Code, e.Status)
	}
	return "databricks: " + e.Code
}

func failure(code string) error { return &Error{Code: code} }

// Credential represents a sanitized authentication token or secret.
// String and GoString representations never disclose its value.
type Credential struct {
	raw  string
	kind string
}

func (Credential) String() string   { return "[Databricks credential]" }
func (Credential) GoString() string { return "[Databricks credential]" }

func PersonalAccessToken(raw string) Credential {
	return Credential{raw: raw, kind: "PAT"}
}

func OAuthM2MToken(raw string) Credential {
	return Credential{raw: raw, kind: "OAUTH_M2M"}
}

func BearerToken(raw string) Credential {
	return Credential{raw: raw, kind: "BEARER"}
}

type TokenProvider func(context.Context) (Credential, error)

// Table maps a trusted application alias to one native table and ID column.
type Table struct {
	Name     string `json:"name"`
	IDColumn string `json:"idColumn"`
}

// Options configure the Databricks client.
type Options struct {
	WarehouseID string
	Catalog     string
	Schema      string
	Tables      map[string]Table
	Token       TokenProvider

	// Transport is cloned and owned. TLS verification is mandatory.
	Transport *http.Transport
	Timeout   time.Duration

	MaxConcurrent   int
	MaxKeys         int
	MaxRows         int
	MaxRequestBytes int
	MaxResponseBytes int
	MaxPageBytes    int
	MaxRequests     int
	MaxColumns      int
	MaxPolls        int

	PollInterval     time.Duration
	ImmediatePolling bool
}

type Column struct {
	Name         string
	Type         string
	Position     int
	Precision    *int64
	Scale        *int64
	Nullable     *bool
	Comment      string
	DefaultValue string
}

// Row contains independently owned exact strings; nil is SQL NULL.
type Row map[string]*string

type Result struct {
	Rows    []Row
	Columns []Column
}

type Document struct {
	ID      string
	Row     Row
	Columns []Column
}

type Metadata struct {
	Catalog          string
	Schema           string
	WarehouseID      string
	Tables           map[string]Table
	Provenance       string
	DynamicDiscovery bool
}

func copyColumns(columns []Column) []Column {
	out := append([]Column(nil), columns...)
	for i := range out {
		x := &out[i]
		if x.Precision != nil {
			v := *x.Precision
			x.Precision = &v
		}
		if x.Scale != nil {
			v := *x.Scale
			x.Scale = &v
		}
		if x.Nullable != nil {
			v := *x.Nullable
			x.Nullable = &v
		}
	}
	return out
}

func copyRow(row Row) Row {
	out := make(Row, len(row))
	for k, p := range row {
		if p == nil {
			out[k] = nil
		} else {
			v := *p
			out[k] = &v
		}
	}
	return out
}

// Databricks Statement Execution API JSON structures
type statementParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"`
}

type statementRequest struct {
	Statement      string               `json:"statement"`
	WarehouseID    string               `json:"warehouse_id"`
	Catalog        string               `json:"catalog,omitempty"`
	Schema         string               `json:"schema,omitempty"`
	Parameters     []statementParameter `json:"parameters,omitempty"`
	Disposition    string               `json:"disposition"`
	Format         string               `json:"format"`
	WaitTimeout    string               `json:"wait_timeout,omitempty"`
	OnWaitTimeout  string               `json:"on_wait_timeout,omitempty"`
}

type statementError struct {
	Message   string `json:"message"`
	ErrorCode string `json:"error_code"`
}

type statementStatus struct {
	State string          `json:"state"`
	Error *statementError `json:"error,omitempty"`
}

type manifestColumn struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeText string `json:"type_text"`
	Position int    `json:"position"`
}

type manifestSchema struct {
	ColumnCount int              `json:"column_count"`
	Columns     []manifestColumn `json:"columns"`
}

type statementManifest struct {
	Format        string         `json:"format"`
	Schema        manifestSchema `json:"schema"`
	TotalRowCount int64          `json:"total_row_count"`
	TotalByteCount int64         `json:"total_byte_count"`
	Truncated     bool           `json:"truncated"`
}

type statementResult struct {
	ChunkIndex            int         `json:"chunk_index"`
	RowCount              int         `json:"row_count"`
	DataArray             [][]any     `json:"data_array"`
	NextChunkIndex        *int        `json:"next_chunk_index,omitempty"`
	NextChunkInternalLink string      `json:"next_chunk_internal_link,omitempty"`
	ExternalLinks         []any       `json:"external_links,omitempty"`
}

type statementResponse struct {
	StatementID string             `json:"statement_id"`
	Status      statementStatus    `json:"status"`
	Manifest    *statementManifest `json:"manifest,omitempty"`
	Result      *statementResult   `json:"result,omitempty"`
}
