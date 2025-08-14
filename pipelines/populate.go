package pipelines

import (
	"github.com/nicola-strappazzon/dacfy/strings"
)

type Populate string

const (
	TypeBackfill Populate = "backfill" // Implemented, means: INSERT ... AS SELECT ...
	TypeChunk    Populate = "chunk"    // Pending, populate via partition. INSERT ... AS SELECT ... WHERE (PARTITION BY)
)

func (p Populate) ToString() string {
	return string(p)
}

func (p Populate) IsEmpty() bool {
	return strings.IsEmpty(p.ToString())
}

func (p Populate) IsNotEmpty() bool {
	return strings.IsNotEmpty(p.ToString())
}

func (p Populate) IsBackFill() bool {
	return p == TypeBackfill
}

func (p Populate) IsNotBackFill() bool {
	return p != TypeBackfill
}
