package pipelines

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultHost = "127.0.0.1:9000"
	defaultUser = "default"
)

type Config struct {
	Debug       bool   // Enable debug mode.
	DryRun      bool   // No execute statement.
	Host        string // ClickHouse server host and port.
	NoCluster   bool   // Ignore ON CLUSTER clauses.
	Password    string // Password for the ClickHouse server.
	Pipe        string // Path to the pipelines file.
	SQL         bool   // Show SQL Statement.
	Suffix      string // Append a suffix to table and view names.
	TLS         bool   // Enable TLS for the ClickHouse server.
	TLSInsecure bool   // Skip TLS certificate verification.
	User        string // Username for the ClickHouse server.
}

func (c *Config) LoadConnectionEnv() error {
	c.Host = envString("DACFY_HOST", defaultHost)
	c.User = envString("DACFY_USER", defaultUser)
	c.Password = envString("DACFY_PASSWORD", "")

	var err error
	if c.TLS, err = envBool("DACFY_TLS", false); err != nil {
		return err
	}

	if c.TLSInsecure, err = envBool("DACFY_TLS_INSECURE", false); err != nil {
		return err
	}

	return nil
}

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}

	return fallback
}

func envBool(name string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}

	out, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}

	return out, nil
}
