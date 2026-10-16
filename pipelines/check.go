package pipelines

import (
	"fmt"
	"math"
)

// Check defines one condition that each non-null field value must satisfy.
// NotNull is a separate rule so null handling is always explicit.
type Check struct {
	Field   Name      `yaml:"field"`
	NotNull bool      `yaml:"not_null"`
	Between []float64 `yaml:"between"`
	GT      *float64  `yaml:"gt"`
	GTE     *float64  `yaml:"gte"`
	LT      *float64  `yaml:"lt"`
	LTE     *float64  `yaml:"lte"`
}

func (c Check) Validate() error {
	if c.Field.IsNotValid() {
		return fmt.Errorf("check.field %q is invalid", c.Field)
	}
	rules := 0
	if c.NotNull {
		rules++
	}
	if c.Between != nil {
		rules++
		if len(c.Between) != 2 || c.Between[0] > c.Between[1] {
			return fmt.Errorf("check.between requires [min, max] with min <= max")
		}
	}
	values := append([]float64(nil), c.Between...)
	for _, bound := range []*float64{c.GT, c.GTE, c.LT, c.LTE} {
		if bound != nil {
			rules++
			values = append(values, *bound)
		}
	}
	if rules != 1 {
		return fmt.Errorf("check requires exactly one of not_null, between, gt, gte, lt or lte")
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("check bounds must be finite numbers")
		}
	}
	return nil
}

// SQL returns the predicate that identifies a rule violation.
func (c Check) SQL() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	if c.NotNull {
		return fmt.Sprintf("%s IS NULL", c.Field), nil
	}
	return fmt.Sprintf("%s IS NOT NULL AND NOT (%s)", c.Field, c.condition()), nil
}

func (c Check) String() string {
	if c.NotNull {
		return fmt.Sprintf("%s not_null", c.Field)
	}
	if c.Between != nil {
		return fmt.Sprintf("%s between [%v, %v]", c.Field, c.Between[0], c.Between[1])
	}
	return c.condition()
}

func (c Check) condition() string {
	switch {
	case c.Between != nil:
		return fmt.Sprintf("%s >= %v AND %s <= %v", c.Field, c.Between[0], c.Field, c.Between[1])
	case c.GT != nil:
		return fmt.Sprintf("%s > %v", c.Field, *c.GT)
	case c.GTE != nil:
		return fmt.Sprintf("%s >= %v", c.Field, *c.GTE)
	case c.LT != nil:
		return fmt.Sprintf("%s < %v", c.Field, *c.LT)
	default:
		return fmt.Sprintf("%s <= %v", c.Field, *c.LTE)
	}
}
