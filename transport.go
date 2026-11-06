package dalgo2databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (c *Client) execute(o *operation, p plan) (*Result, error) {
	cred, err := o.auth()
	if err != nil {
		return nil, err
	}

	reqBody := statementRequest{
		Statement:     p.sql,
		WarehouseID:   c.options.WarehouseID,
		Catalog:       c.options.Catalog,
		Schema:        c.options.Schema,
		Parameters:    p.parameters,
		Disposition:   "INLINE",
		Format:        "JSON_ARRAY",
		WaitTimeout:   "30s",
		OnWaitTimeout: "CONTINUE",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, failure("input")
	}
	if len(bodyBytes) > c.options.MaxRequestBytes {
		return nil, failure("bounds")
	}

	url := c.endpoint + "/api/2.0/sql/statements"
	respData, err := c.postJSON(o, cred, url, bodyBytes)
	if err != nil {
		return nil, err
	}

	var stmtResp statementResponse
	if err := json.Unmarshal(respData, &stmtResp); err != nil {
		return nil, failure("format")
	}

	if stmtResp.StatementID != "" {
		o.setStatementID(stmtResp.StatementID)
	}

	// Poll if query is still pending or running
	polls := 0
	for stmtResp.Status.State == "PENDING" || stmtResp.Status.State == "RUNNING" {
		polls++
		if polls > c.options.MaxPolls {
			c.cancelStatement(context.Background(), cred, stmtResp.StatementID)
			return nil, failure("timeout")
		}

		if !c.options.ImmediatePolling {
			select {
			case <-o.ctx.Done():
				c.cancelStatement(context.Background(), cred, stmtResp.StatementID)
				return nil, safeContext(o.ctx)
			case <-time.After(c.options.PollInterval):
			}
		}

		pollURL := fmt.Sprintf("%s/api/2.0/sql/statements/%s", c.endpoint, stmtResp.StatementID)
		pollData, err := c.getJSON(o, cred, pollURL)
		if err != nil {
			c.cancelStatement(context.Background(), cred, stmtResp.StatementID)
			return nil, err
		}

		if err := json.Unmarshal(pollData, &stmtResp); err != nil {
			c.cancelStatement(context.Background(), cred, stmtResp.StatementID)
			return nil, failure("format")
		}
	}

	if stmtResp.Status.State != "SUCCEEDED" {
		if stmtResp.Status.State == "CANCELED" {
			return nil, context.Canceled
		}
		return nil, failure("execution")
	}

	return c.parseResult(o, cred, &stmtResp)
}

func (c *Client) parseResult(o *operation, cred Credential, stmtResp *statementResponse) (*Result, error) {
	if stmtResp.Manifest == nil {
		return nil, failure("format")
	}

	columns := make([]Column, len(stmtResp.Manifest.Schema.Columns))
	for i, col := range stmtResp.Manifest.Schema.Columns {
		columns[i] = Column{
			Name:     col.Name,
			Type:     col.TypeName,
			Position: col.Position,
		}
	}

	var rows []Row
	totalBytes := 0

	if stmtResp.Result != nil && len(stmtResp.Result.DataArray) > 0 {
		for _, dataRow := range stmtResp.Result.DataArray {
			if len(rows) >= c.options.MaxRows {
				return nil, failure("bounds")
			}
			row := make(Row, len(columns))
			for i, col := range columns {
				if i < len(dataRow) && dataRow[i] != nil {
					strVal := fmt.Sprint(dataRow[i])
					totalBytes += len(strVal)
					if totalBytes > c.options.MaxResponseBytes {
						return nil, failure("bounds")
					}
					row[col.Name] = &strVal
				} else {
					row[col.Name] = nil
				}
			}
			rows = append(rows, row)
		}

		// Handle pagination / subsequent chunks if any
		nextChunk := stmtResp.Result.NextChunkIndex
		for nextChunk != nil {
			chunkURL := fmt.Sprintf("%s/api/2.0/sql/statements/%s/result/chunks/%d", c.endpoint, stmtResp.StatementID, *nextChunk)
			chunkData, err := c.getJSON(o, cred, chunkURL)
			if err != nil {
				return nil, err
			}

			var chunkResult statementResult
			if err := json.Unmarshal(chunkData, &chunkResult); err != nil {
				return nil, failure("format")
			}

			for _, dataRow := range chunkResult.DataArray {
				if len(rows) >= c.options.MaxRows {
					return nil, failure("bounds")
				}
				row := make(Row, len(columns))
				for i, col := range columns {
					if i < len(dataRow) && dataRow[i] != nil {
						strVal := fmt.Sprint(dataRow[i])
						totalBytes += len(strVal)
						if totalBytes > c.options.MaxResponseBytes {
							return nil, failure("bounds")
						}
						row[col.Name] = &strVal
					} else {
						row[col.Name] = nil
					}
				}
				rows = append(rows, row)
			}
			nextChunk = chunkResult.NextChunkIndex
		}
	}

	return &Result{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (c *Client) postJSON(o *operation, cred Credential, targetURL string, body []byte) ([]byte, error) {
	if err := o.check(); err != nil {
		return nil, err
	}

	return foreign(o, o.ctx, func() ([]byte, error) {
		req, err := http.NewRequestWithContext(o.ctx, http.MethodPost, targetURL, bytes.NewReader(body))
		if err != nil {
			return nil, failure("input")
		}
		req.Header.Set("Authorization", "Bearer "+cred.raw)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "dalgo2databricks/0.1.0")

		client := &http.Client{
			Transport: c.transport,
			Timeout:   c.options.Timeout,
		}

		resp, err := client.Do(req)
		if err != nil {
			if o.ctx.Err() != nil {
				return nil, safeContext(o.ctx)
			}
			return nil, failure("network")
		}
		defer resp.Body.Close()

		respBytes, err := io.ReadAll(io.LimitReader(resp.Body, int64(c.options.MaxResponseBytes+1024)))
		if err != nil {
			return nil, failure("network")
		}

		if resp.StatusCode >= 400 {
			return nil, &Error{Code: "http_error", Status: resp.StatusCode}
		}

		return respBytes, nil
	})
}

func (c *Client) getJSON(o *operation, cred Credential, targetURL string) ([]byte, error) {
	if err := o.check(); err != nil {
		return nil, err
	}

	return foreign(o, o.ctx, func() ([]byte, error) {
		req, err := http.NewRequestWithContext(o.ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, failure("input")
		}
		req.Header.Set("Authorization", "Bearer "+cred.raw)
		req.Header.Set("User-Agent", "dalgo2databricks/0.1.0")

		client := &http.Client{
			Transport: c.transport,
			Timeout:   c.options.Timeout,
		}

		resp, err := client.Do(req)
		if err != nil {
			if o.ctx.Err() != nil {
				return nil, safeContext(o.ctx)
			}
			return nil, failure("network")
		}
		defer resp.Body.Close()

		respBytes, err := io.ReadAll(io.LimitReader(resp.Body, int64(c.options.MaxResponseBytes+1024)))
		if err != nil {
			return nil, failure("network")
		}

		if resp.StatusCode >= 400 {
			return nil, &Error{Code: "http_error", Status: resp.StatusCode}
		}

		return respBytes, nil
	})
}

func (c *Client) cancelStatement(ctx context.Context, cred Credential, statementID string) {
	if statementID == "" {
		return
	}
	cancelURL := fmt.Sprintf("%s/api/2.0/sql/statements/%s/cancel", c.endpoint, statementID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cancelURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+cred.raw)
	req.Header.Set("User-Agent", "dalgo2databricks/0.1.0")

	client := &http.Client{
		Transport: c.transport,
		Timeout:   5 * time.Second,
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}
