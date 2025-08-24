package pipelines

import (
	"github.com/nicola-strappazzon/dacfy/minify"
	"github.com/nicola-strappazzon/dacfy/strings"
)

type Query string

func (q Query) ToString() string {
	return strings.TrimSpace(string(q))
}

func (q Query) IsEmpty() bool {
	return strings.IsEmpty(q.ToString())
}

func (q Query) IsNotEmpty() bool {
	return !q.IsEmpty()
}

func (q Query) Minify() string {
	return minify.Minify(q.ToString())
}
