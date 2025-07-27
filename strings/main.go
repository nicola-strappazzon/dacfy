package strings

import (
	"strings"
)

func Join(in []string) string {
	return strings.Join(in[:], ",")
}

func IsEmpty(in string) bool {
	return !IsNotEmpty(in)
}

func IsNotEmpty(in string) bool {
	return !(strings.TrimSpace(in) == "")
}
