package strings

import (
	"strings"
)

func Join(in []string) string {
	return strings.Join(in[:], ",")
}

func TrimSpace(in string) string {
	return strings.TrimSpace(in)
}

func IsEmpty(in string) bool {
	return !IsNotEmpty(in)
}

func IsNotEmpty(in string) bool {
	return !(strings.TrimSpace(in) == "")
}

func Contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func ReplaceAll(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

func TrimRight(s, cutset string) string {
	return strings.TrimRight(s, cutset)
}
