package dalgo2databricks

import (
	"context"
	"time"
)

// Shutdown cancels active statements and waits up to the provided context deadline
// for local operations to become quiescent.
func (c *Client) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	c.closed = true
	var activeOps []*operation
	for o := range c.active {
		activeOps = append(activeOps, o)
	}
	c.mu.Unlock()

	// Cancel statements on Databricks warehouse
	for _, o := range activeOps {
		stmtID := o.getStatementID()
		if stmtID != "" {
			c.cancelStatement(ctx, Credential{raw: "shutdown"}, stmtID)
		}
		o.cancel()
	}

	// Drain active operations
	for {
		c.mu.Lock()
		count := len(c.active)
		changed := c.stateChanged
		c.mu.Unlock()

		if count == 0 {
			break
		}

		select {
		case <-ctx.Done():
			c.owned.CloseIdleConnections()
			return ctx.Err()
		case <-changed:
		case <-time.After(25 * time.Millisecond):
		}
	}

	c.owned.CloseIdleConnections()
	return nil
}
