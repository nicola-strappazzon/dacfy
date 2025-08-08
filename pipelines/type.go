package pipelines

type PopulateType string

const (
	PopulateNative   PopulateType = "native"   // Implemented, means: CREATE MATERIALIZED VIEW ... POPULATE AS SELECT ...
	PopulateBackFill PopulateType = "backfill" // Implemented, means: INSERT ... AS SELECT ...
	PopulateChunk    PopulateType = "chunk"    // Pending, populate via partition. INSERT ... AS SELECT ... WHERE (PARTITION BY)
)

type Populate struct {
	// CutOff string       `yaml:"cutoff"`
	Skip bool         `yaml:"skip"`
	Type PopulateType `yaml:"type"`
}
