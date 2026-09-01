package pipelines

import (
	gstrings "strings"

	"github.com/nicola-strappazzon/dacfy/strings"
)

type Engine string

func (e Engine) ToString() string {
	return strings.TrimSpace(string(e))
}

func (e Engine) IsEmpty() bool {
	return strings.IsEmpty(e.ToString())
}

func (e Engine) IsNotEmpty() bool {
	return !e.IsEmpty()
}

func (e Engine) IsMongoDB() bool {
	return e.ToString() == "MongoDB"
}

// WithoutReplicated converts a ReplicatedXxxMergeTree engine into its
// non-replicated XxxMergeTree counterpart, dropping the ZooKeeper path and
// replica name (the first two positional arguments) while keeping the rest.
// Engines that are not replicated are returned unchanged.
func (e Engine) WithoutReplicated() Engine {
	s := e.ToString()

	if !gstrings.HasPrefix(s, "Replicated") {
		return e
	}

	base := gstrings.TrimPrefix(s, "Replicated")

	open := gstrings.Index(base, "(")
	if open == -1 {
		return Engine(base)
	}

	name := gstrings.TrimSpace(base[:open])
	inner := base[open+1:]

	if close := gstrings.LastIndex(inner, ")"); close != -1 {
		inner = inner[:close]
	}

	args := splitTopLevel(inner)
	if len(args) <= 2 {
		return Engine(name)
	}

	return Engine(name + "(" + gstrings.Join(args[2:], ", ") + ")")
}

// splitTopLevel splits a comma-separated argument list, ignoring commas that
// appear inside quotes or nested parentheses.
func splitTopLevel(in string) []string {
	var args []string
	var buf gstrings.Builder
	depth := 0
	quoted := false

	for _, r := range in {
		switch {
		case r == '\'':
			quoted = !quoted
			buf.WriteRune(r)
		case quoted:
			buf.WriteRune(r)
		case r == '(':
			depth++
			buf.WriteRune(r)
		case r == ')':
			depth--
			buf.WriteRune(r)
		case r == ',' && depth == 0:
			args = append(args, gstrings.TrimSpace(buf.String()))
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}

	if gstrings.TrimSpace(buf.String()) != "" {
		args = append(args, gstrings.TrimSpace(buf.String()))
	}

	return args
}
