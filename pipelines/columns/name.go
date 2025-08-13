package columns

import (
	"regexp"
)

type Name string

func (n Name) ToString() string {
	return string(n)
}

func (n Name) Clear() string {
	re := regexp.MustCompile(`^(?:\w+\()?([a-zA-Z_][a-zA-Z0-9_]*)\)?$`)

	match := re.FindStringSubmatch(n.ToString())
	if len(match) > 1 {
		return match[1]
	}

	return n.ToString()
}
