package pipelines

import (
	"strings"

	"github.com/nicola-strappazzon/clickhouse-dac/minify"
)

type Query string

func (q Query) String() string {
	return strings.TrimSpace(string(q))
}

func (q Query) Minify() string {
	return minify.Minify(q.String())
}
