package dalgo2databricks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdown_DrainsAndCancels(t *testing.T) {
	var cancelCalled int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/2.0/sql/statements" {
			// Query takes long time
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/api/2.0/sql/statements/stmt-active/cancel" {
			atomic.AddInt32(&cancelCalled, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
	}))
	defer ts.Close()

	opts := validTestOptions()
	c, err := New(ts.URL, opts)
	require.NoError(t, err)

	// Simulate active operation with statement ID
	o, err := c.begin(context.Background())
	require.NoError(t, err)
	o.setStatementID("stmt-active")

	go func() {
		time.Sleep(50 * time.Millisecond)
		o.finish()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = c.Shutdown(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&cancelCalled), int32(1))
}
