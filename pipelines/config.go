package pipelines

import (
	"github.com/nicola-strappazzon/dacfy/env"
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

func (c *Config) Load() error {
	c.Host = env.String("DACFY_HOST", defaultHost)
	c.User = env.String("DACFY_USER", defaultUser)
	c.Password = env.String("DACFY_PASSWORD", "")

	var err error
	if c.TLS, err = env.Bool("DACFY_TLS", false); err != nil {
		return err
	}

	if c.TLSInsecure, err = env.Bool("DACFY_TLS_INSECURE", false); err != nil {
		return err
	}

	return nil
}
