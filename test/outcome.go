package test

import "fmt"

type outcome struct {
	detail string
	key    string
	pass   bool
}

func (o outcome) String() string {
	status := "PASS"
	if !o.pass {
		status = "FAIL"
	}
	if o.detail != "" {
		return fmt.Sprintf("    %s: %s (%s)", o.key, status, o.detail)
	}
	return fmt.Sprintf("    %s: %s", o.key, status)
}
