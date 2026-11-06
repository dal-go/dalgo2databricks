package dalgo2databricks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validTestOptions() Options {
	return Options{
		WarehouseID: "wh-12345",
		Catalog:     "main",
		Schema:      "analytics",
		Tables: map[string]Table{
			"users": {Name: "users", IDColumn: "id"},
		},
		Token: func(ctx context.Context) (Credential, error) {
			return PersonalAccessToken("dapi_test_token_12345"), nil
		},
		Timeout: 30 * time.Second,
	}
}

func TestNew_ValidHosts(t *testing.T) {
	hosts := []string{
		"https://dbc-12345678-abcd.cloud.databricks.com",
		"https://adb-1234567890123456.7.azuredatabricks.net",
		"https://1234567890.gcp.databricks.com",
		"https://custom-databricks.corp.internal",
		"https://localhost",
		"https://127.0.0.1",
	}

	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			c, err := New(host, validTestOptions())
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	opts := validTestOptions()

	// Invalid scheme
	_, err := New("ftp://dbc-1234.cloud.databricks.com", opts)
	assert.Error(t, err)

	// User info forbidden
	_, err = New("https://user:pass@dbc-1234.cloud.databricks.com", opts)
	assert.Error(t, err)

	// Query params forbidden
	_, err = New("https://dbc-1234.cloud.databricks.com?foo=bar", opts)
	assert.Error(t, err)

	// Fragment forbidden
	_, err = New("https://dbc-1234.cloud.databricks.com#section", opts)
	assert.Error(t, err)

	// Host not recognized
	_, err = New("https://unrelated-database.example.com", opts)
	assert.Error(t, err)

	// Missing warehouse ID
	badOpts := opts
	badOpts.WarehouseID = ""
	_, err = New("https://dbc-1234.cloud.databricks.com", badOpts)
	assert.Error(t, err)

	// Nil token provider
	badOpts = opts
	badOpts.Token = nil
	_, err = New("https://dbc-1234.cloud.databricks.com", badOpts)
	assert.Error(t, err)

	// Invalid timeout
	badOpts = opts
	badOpts.Timeout = -1
	_, err = New("https://dbc-1234.cloud.databricks.com", badOpts)
	assert.Error(t, err)

	// Invalid table config
	badOpts = opts
	badOpts.Tables = map[string]Table{"bad": {Name: "", IDColumn: "id"}}
	_, err = New("https://dbc-1234.cloud.databricks.com", badOpts)
	assert.Error(t, err)
}

func TestCredential_Redaction(t *testing.T) {
	cred := PersonalAccessToken("secret-pat-12345")
	assert.Equal(t, "[Databricks credential]", cred.String())
	assert.Equal(t, "[Databricks credential]", cred.GoString())

	credM2M := OAuthM2MToken("secret-m2m-token-67890")
	assert.Equal(t, "[Databricks credential]", credM2M.String())

	credBearer := BearerToken("secret-bearer-999")
	assert.Equal(t, "[Databricks credential]", credBearer.String())
}

func TestError_Formatting(t *testing.T) {
	e := failure("config")
	assert.Equal(t, "databricks: config", e.Error())

	eWithStatus := &Error{Code: "not_found", Status: 404}
	assert.Equal(t, "databricks: not_found (HTTP 404)", eWithStatus.Error())
}
