package columns

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Array []Name

func (c Array) ToArrayString() []string {
	r := make([]string, len(c))
	for i, val := range c {
		r[i] = val.ToString()
	}
	return r
}

func (c Array) Join() string {
	return strings.Join(c.ToArrayString())
}

func (c Array) IsEmpty() bool {
	return len(c) == 0
}

func (c Array) IsNotEmpty() bool {
	return !c.IsEmpty()
}

func (c Array) NotIn(in Array) ([]string, bool) {
	var d = []string{}

	for _, x := range c {
		f := false
		for _, y := range in {
			if x.Clear() == y.Clear() {
				f = true
				break
			}
		}
		if !f {
			d = append(d, x.Clear())
		}
	}

	return d, len(d) > 0
}
