package dalgo2databricks

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Client struct {
	endpoint     string
	options      Options
	transport    http.RoundTripper
	owned        *http.Transport
	mu           sync.Mutex
	closed       bool
	active       map[*operation]struct{}
	stateChanged chan struct{}
}

func name(s string) bool {
	return s != "" && len(s) <= 4096 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

func bounded(v *int, def, max int) bool {
	if *v == 0 {
		*v = def
	}
	return *v > 0 && *v <= max
}

// isDatabricksHostname verifies the host is a valid Databricks domain (AWS, Azure, GCP, or private).
func isDatabricksHostname(host string) bool {
	h := strings.ToLower(host)
	if h == "" || strings.Contains(h, ":") {
		return false
	}
	// AWS: *.cloud.databricks.com
	// Azure: adb-*.azuredatabricks.net or *.azuredatabricks.net
	// GCP: *.*.gcp.databricks.com or *.gcp.databricks.com
	// PrivateLink / Local fixtures: localhost, 127.0.0.1, or corporate internal domains
	return strings.HasSuffix(h, ".databricks.com") ||
		strings.HasSuffix(h, ".azuredatabricks.net") ||
		strings.HasSuffix(h, ".databricks.net") ||
		strings.Contains(h, "databricks") ||
		h == "localhost" ||
		h == "127.0.0.1"
}

// New initializes a Databricks client. Connection and credential calls are lazy.
func New(endpoint string, options Options) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
		return nil, failure("config")
	}
	if !isDatabricksHostname(u.Hostname()) {
		return nil, failure("config")
	}
	if !name(options.WarehouseID) || options.Token == nil {
		return nil, failure("config")
	}
	if options.Catalog != "" && !name(options.Catalog) {
		return nil, failure("config")
	}
	if options.Schema != "" && !name(options.Schema) {
		return nil, failure("config")
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	if options.Timeout <= 0 || options.Timeout > 10*time.Minute {
		return nil, failure("config")
	}
	if !bounded(&options.MaxConcurrent, 4, 32) ||
		!bounded(&options.MaxKeys, 100, 1000) ||
		!bounded(&options.MaxRows, 10000, 100000) ||
		!bounded(&options.MaxRequestBytes, 64*1024, 4*1024*1024) ||
		!bounded(&options.MaxResponseBytes, 16*1024*1024, 64*1024*1024) ||
		!bounded(&options.MaxPageBytes, 4*1024*1024, 16*1024*1024) ||
		!bounded(&options.MaxRequests, 512, 4096) ||
		!bounded(&options.MaxColumns, 512, 4096) ||
		!bounded(&options.MaxPolls, 240, 1000) {
		return nil, failure("config")
	}
	if options.PollInterval == 0 && !options.ImmediatePolling {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.PollInterval < 0 || options.PollInterval > options.Timeout {
		return nil, failure("config")
	}

	tables := make(map[string]Table, len(options.Tables))
	for alias, t := range options.Tables {
		if !name(alias) || !name(t.Name) || !name(t.IDColumn) {
			return nil, failure("config")
		}
		tables[alias] = t
	}
	options.Tables = tables

	var t *http.Transport
	if options.Transport != nil {
		t = options.Transport.Clone()
	} else {
		t = &http.Transport{DialContext: (&net.Dialer{Timeout: options.Timeout}).DialContext}
	}
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{}
	} else {
		t.TLSClientConfig = t.TLSClientConfig.Clone()
		if t.TLSClientConfig.RootCAs != nil {
			t.TLSClientConfig.RootCAs = t.TLSClientConfig.RootCAs.Clone()
		}
	}
	t.TLSClientConfig.MinVersion = max(t.TLSClientConfig.MinVersion, tls.VersionTLS12)
	t.DisableKeepAlives = true
	t.DisableCompression = true
	t.MaxResponseHeaderBytes = 64 * 1024
	t.MaxConnsPerHost = options.MaxConcurrent * 2
	t.ResponseHeaderTimeout = options.Timeout

	baseEndpoint := u.Scheme + "://" + u.Host
	return &Client{
		endpoint:     baseEndpoint,
		options:      options,
		transport:    t,
		owned:        t,
		active:       make(map[*operation]struct{}),
		stateChanged: make(chan struct{}),
	}, nil
}

type operation struct {
	client      *Client
	parent, ctx context.Context
	cancel      context.CancelFunc
	deadline    time.Time
	physical    int
	finished    bool
	statementID string
	mu          sync.Mutex
}

func (c *Client) begin(parent context.Context) (*operation, error) {
	if parent == nil {
		return nil, failure("input")
	}
	if err := safeContext(parent); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, c.options.Timeout)
	deadline, _ := ctx.Deadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		cancel()
		return nil, failure("closed")
	}
	if len(c.active) >= c.options.MaxConcurrent {
		cancel()
		return nil, failure("busy")
	}
	o := &operation{client: c, parent: parent, ctx: ctx, cancel: cancel, deadline: deadline}
	c.active[o] = struct{}{}
	return o, nil
}

func safeContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	return nil
}

func (o *operation) check() error {
	if err := safeContext(o.parent); err != nil {
		return err
	}
	if !time.Now().Before(o.deadline) {
		return context.DeadlineExceeded
	}
	o.client.mu.Lock()
	closed := o.client.closed
	o.client.mu.Unlock()
	if closed {
		return failure("closed")
	}
	return safeContext(o.ctx)
}

func (o *operation) setStatementID(id string) {
	o.mu.Lock()
	o.statementID = id
	o.mu.Unlock()
}

func (o *operation) getStatementID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.statementID
}

func (o *operation) releaseLocked() {
	if o.finished && o.physical == 0 {
		if _, present := o.client.active[o]; present {
			delete(o.client.active, o)
			close(o.client.stateChanged)
			o.client.stateChanged = make(chan struct{})
		}
	}
}

func (o *operation) track() {
	o.client.mu.Lock()
	o.physical++
	o.client.mu.Unlock()
}

func (o *operation) settled() {
	o.client.mu.Lock()
	o.physical--
	o.releaseLocked()
	o.client.mu.Unlock()
}

func (o *operation) finish() {
	o.client.mu.Lock()
	o.finished = true
	o.releaseLocked()
	o.client.mu.Unlock()
	o.cancel()
}

type foreignResult[T any] struct {
	value T
	err   error
}

func foreign[T any](o *operation, ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	o.track()
	ch := make(chan foreignResult[T], 1)
	go func() {
		v, e := fn()
		o.settled()
		ch <- foreignResult[T]{v, e}
	}()
	select {
	case <-ctx.Done():
		return zero, safeContext(ctx)
	case r := <-ch:
		return r.value, r.err
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for o := range c.active {
			o.cancel()
		}
	}
	c.mu.Unlock()
	c.owned.CloseIdleConnections()
	return nil
}

func (o *operation) auth() (Credential, error) {
	if e := o.check(); e != nil {
		return Credential{}, e
	}
	cred, e := foreign(o, o.ctx, func() (Credential, error) {
		if e := o.check(); e != nil {
			return Credential{}, e
		}
		return o.client.options.Token(o.ctx)
	})
	if final := o.check(); final != nil {
		return Credential{}, final
	}
	if e != nil {
		return Credential{}, failure("auth")
	}
	if len(cred.raw) == 0 || len(cred.raw) > 16384 {
		return Credential{}, failure("auth")
	}
	for _, r := range cred.raw {
		if r < 32 || r > 126 {
			return Credential{}, failure("auth")
		}
	}
	return cred, nil
}
