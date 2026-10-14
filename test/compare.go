package test

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type comparison struct {
	pipeline pipelines.Test
	result   clickhouse.Result
}

func (c comparison) Outcomes() ([]outcome, error) {
	keyIndexes, err := c.keyIndexes()
	if err != nil {
		return nil, err
	}

	actual, err := c.actualRows(keyIndexes)
	if err != nil {
		return nil, err
	}

	outcomes := make([]outcome, 0, len(c.pipeline.Expected))
	seen := make(map[string]bool, len(c.pipeline.Expected))
	for _, row := range c.pipeline.Expected {
		values := c.expectedRow(row)
		valueKey := values.Key(keyIndexes)
		if seen[valueKey] {
			return nil, errors.New("test.expected contains duplicate key values")
		}
		seen[valueKey] = true
		item := c.compare(values, actual[valueKey])
		item.key = key(c.pipeline.Key).Format(row)
		outcomes = append(outcomes, item)
		delete(actual, valueKey)
	}
	keys := make([]string, 0, len(actual))
	for key := range actual {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		outcomes = append(outcomes, outcome{key: key, detail: "unexpected row"})
	}
	return outcomes, nil
}

func (c comparison) keyIndexes() ([]int, error) {
	indexes := make([]int, len(c.pipeline.Key))
	for i, key := range c.pipeline.Key {
		index := -1
		for columnIndex, column := range c.result.Columns {
			if column.Name == key {
				index = columnIndex
				break
			}
		}
		if index == -1 {
			return nil, fmt.Errorf("key %q was not returned", key)
		}
		indexes[i] = index
	}
	return indexes, nil
}

func (c comparison) actualRows(keyIndexes []int) (map[string][]clickhouse.Values, error) {
	rows := make(map[string][]clickhouse.Values)
	for _, row := range c.result.Rows {
		values, err := c.actualRow(row)
		if err != nil {
			return nil, err
		}
		key := values.Key(keyIndexes)
		rows[key] = append(rows[key], values)
	}
	return rows, nil
}

func (c comparison) compare(expected clickhouse.Values, actual []clickhouse.Values) outcome {
	switch len(actual) {
	case 0:
		return outcome{detail: "missing"}
	case 1:
		if expected.Equal(actual[0]) {
			return outcome{pass: true}
		}
		return outcome{detail: "values differ"}
	default:
		return outcome{detail: fmt.Sprintf("found %d rows", len(actual))}
	}
}

func (c comparison) expectedRow(row map[string]any) clickhouse.Values {
	values := make(clickhouse.Values, len(c.result.Columns))
	for i, column := range c.result.Columns {
		values[i] = clickhouse.NewValue(row[column.Name])
	}
	return values
}

func (c comparison) actualRow(row clickhouse.Values) (clickhouse.Values, error) {
	if len(row) != len(c.result.Columns) {
		return nil, errors.New("query returned an unexpected number of values")
	}
	return row, nil
}
