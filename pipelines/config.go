package pipelines

type Config struct {
	Host     string // ClickHouse server host and port.
	Password string // Password for the ClickHouse server.
	Pipe     string // Path to the pipelines file.
	SQL      bool   // Show SQL Statement.
	TLS      bool   // Enable TLS for the ClickHouse server.
	User     string // Username for the ClickHouse server.
}
