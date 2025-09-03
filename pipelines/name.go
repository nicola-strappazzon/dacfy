package pipelines

import (
	"regexp"

	"github.com/nicola-strappazzon/dacfy/strings"
)

type Name string

func (n Name) ToString() string {
	return string(n)
}

func (n Name) IsEmpty() bool {
	return strings.IsEmpty(n.ToString())
}

func (n Name) IsNotEmpty() bool {
	return !n.IsEmpty()
}

func (n Name) IsNotValid() bool {
	var re = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{1,254}$`)

	return !re.MatchString(n.ToString())
}

func (n Name) Suffix(in string) Name {
	if n.IsEmpty() {
		return n
	}

	return Name(n.ToString() + in)
}
