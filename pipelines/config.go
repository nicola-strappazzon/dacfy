package pipelines

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
