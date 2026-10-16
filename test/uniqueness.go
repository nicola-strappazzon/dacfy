package test

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
)

type uniqueness struct {
	fields key
	result clickhouse.Result
}

func (u uniqueness) Outcomes() ([]outcome, error) {
	if len(u.result.Rows) == 0 {
		return []outcome{{key: "unique (" + strings.Join(u.fields, ", ") + ")", pass: true}}, nil
	}
	outcomes := make([]outcome, 0, len(u.result.Rows))
	for _, row := range u.result.Rows {
		if len(row) != len(u.fields)+1 {
			return nil, errors.New("uniqueness query returned an unexpected number of values")
		}
		outcomes = append(outcomes, outcome{
			key:    u.fields.FormatValues(row),
			detail: fmt.Sprintf("found %s rows", row[len(u.fields)].Text()),
		})
	}
	return outcomes, nil
}
