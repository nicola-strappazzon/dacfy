package pipelines

import "fmt"

// RowCount defines the number of rows a test query must return.
type RowCount struct {
	EQ  *uint64 `yaml:"eq"`
	GTE *uint64 `yaml:"gte"`
	LTE *uint64 `yaml:"lte"`
}

func (r RowCount) Validate() error {
	if r.EQ != nil && (r.GTE != nil || r.LTE != nil) {
		return fmt.Errorf("rows.eq cannot be combined with gte or lte")
	}
	if r.EQ == nil && r.GTE == nil && r.LTE == nil {
		return fmt.Errorf("rows requires eq, gte or lte")
	}
	if r.GTE != nil && r.LTE != nil && *r.GTE > *r.LTE {
		return fmt.Errorf("rows.gte must be less than or equal to rows.lte")
	}
	return nil
}

func (r RowCount) Match(count uint64) bool {
	if r.EQ != nil {
		return count == *r.EQ
	}
	return (r.GTE == nil || count >= *r.GTE) && (r.LTE == nil || count <= *r.LTE)
}

func (r RowCount) String() string {
	switch {
	case r.EQ != nil:
		return fmt.Sprintf("= %d", *r.EQ)
	case r.GTE != nil && r.LTE != nil:
		return fmt.Sprintf("between [%d, %d]", *r.GTE, *r.LTE)
	case r.GTE != nil:
		return fmt.Sprintf(">= %d", *r.GTE)
	default:
		return fmt.Sprintf("<= %d", *r.LTE)
	}
}
