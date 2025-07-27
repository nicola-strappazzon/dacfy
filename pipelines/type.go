package pipelines

type PopulateType string

const (
	PopulateNative PopulateType = "native" // Implemented, means: CREATE MATERIALIZED VIEW ... POPULATE AS SELECT ...
	PopulateQuery  PopulateType = "query"  // Implemented, means: INSERT ... AS SELECT ...
	PopulateChunk  PopulateType = "chunk"  // Pending, populate via partition.
)

type Populate struct {
	Skip bool         `yaml:"skip"`
	Type PopulateType `yaml:"type"`
}
