package columns

import (
	"github.com/nicola-strappazzon/dacfy/strings"
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

func (c Array) First() Name {
	if c.Count() == 1 {
		return c[0]
	}

	return Name("")
}

func (c Array) Count() int {
	return len(c)
}

func (c Array) IsEmpty() bool {
	if c.Count() == 1 && c.First().Clear() == "" {
		return true
	}

	return c.Count() == 0
}

func (c Array) IsNotEmpty() bool {
	return !c.IsEmpty()
}

func (c Array) NotIn(in Array) ([]string, bool) {
	var d = []string{}

	if c.IsEmpty() {
		return d, false
	}

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
