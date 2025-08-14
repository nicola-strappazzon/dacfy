package pipelines

import (
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
