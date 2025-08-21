package columns

import (
	"regexp"
)

type Name string

func (n Name) ToString() string {
	return string(n)
}

func (n Name) Clear() (out string) {
	if n.IsTuple() {
		return out
	}

	if ok, arg := n.ExtractArgument(); ok {
		return arg
	}

	return n.ToString()
}

func (n Name) ExtractArgument() (bool, string) {
	re := regexp.MustCompile(`^(?:\w+\()?([a-zA-Z_][a-zA-Z0-9_]*)\)?$`)

	match := re.FindStringSubmatch(n.ToString())
	if len(match) > 1 {
		return true, match[1]
	}

	return false, ""
}

func (n Name) IsTuple() bool {
	return n.ToString() == "tuple()"
}
