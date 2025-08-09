package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/minify"
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Query string

func (q Query) ToString() string {
	return strings.TrimSpace(string(q))
}

func (q Query) IsEmpty() bool {
	return strings.IsEmpty(q.ToString())
}

func (q Query) Minify() string {
	return minify.Minify(q.ToString())
}
