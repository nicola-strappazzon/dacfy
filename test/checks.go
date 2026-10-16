package test

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type checks struct {
	rules  []pipelines.Check
	result clickhouse.Result
}

func (c checks) Outcomes() ([]outcome, error) {
	if len(c.result.Rows) != 1 || len(c.result.Rows[0]) != len(c.rules) {
		return nil, errors.New("checks query returned unexpected counts")
	}
	outcomes := make([]outcome, len(c.rules))
	for i, rule := range c.rules {
		count, err := strconv.ParseUint(c.result.Rows[0][i].Text(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("check %q: invalid violation count: %w", rule.String(), err)
		}
		outcomes[i] = outcome{key: rule.String(), pass: count == 0}
		if count > 0 {
			outcomes[i].detail = fmt.Sprintf("%d rows", count)
		}
	}
	return outcomes, nil
}
