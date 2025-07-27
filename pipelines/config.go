package pipelines

type Config struct {
	Pipe     string // Path to the pipelines file.
	Host     string // ClickHouse server host and port.
	User     string // Username for the ClickHouse server.
	Password string // Password for the ClickHouse server.
	TLS      bool   // Enable TLS for the ClickHouse server.
}
