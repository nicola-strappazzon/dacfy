package pipelines

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	stdstrings "strings"
	"time"
)

type Test struct {
	Checks   []Check          `yaml:"checks"`
	Expected []map[string]any `yaml:"expected"`
	Key      []string         `yaml:"key"`
	Name     Name             `yaml:"name"`
	Table    Name             `yaml:"table"`
	Unique   []string         `yaml:"unique"`
	Where    Where            `yaml:"where"`
}

func (t Test) SetSuffix(in string) Test {
	t.Table = t.Table.Suffix(in)
	return t
}

func (t Test) Validate() error {
	if t.Name.IsNotValid() {
		return fmt.Errorf("test.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", t.Name.ToString())
	}

	if t.Table.IsNotValid() {
		return fmt.Errorf("test.table %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", t.Table.ToString())
	}

	if t.Unique != nil {
		if t.Key != nil || t.Expected != nil || t.Checks != nil {
			return fmt.Errorf("test.unique cannot be combined with key, expected or checks")
		}
		if len(t.Unique) == 0 {
			return fmt.Errorf("test.unique requires at least one field")
		}
		return t.validateFields(t.Unique)
	}
	if t.Checks != nil {
		if t.Key != nil || t.Expected != nil {
			return fmt.Errorf("test.checks cannot be combined with key or expected")
		}
		if len(t.Checks) == 0 {
			return fmt.Errorf("test.checks requires at least one rule")
		}
		for i, check := range t.Checks {
			if err := check.Validate(); err != nil {
				return fmt.Errorf("test.checks[%d]: %w", i, err)
			}
		}
		return nil
	}
	return t.validateExpected()
}

func (t Test) validateExpected() error {
	if len(t.Key) == 0 {
		return fmt.Errorf("test.key is required")
	}

	if len(t.Expected) == 0 {
		return fmt.Errorf("test.expected is required")
	}

	if err := t.validateFields(t.Key); err != nil {
		return err
	}

	columns := t.columns()
	if len(columns) == 0 {
		return fmt.Errorf("test.expected rows must define at least one column")
	}

	for _, key := range t.Key {
		if _, ok := t.Expected[0][key]; !ok {
			return fmt.Errorf("test.key %q is not present in expected rows", key)
		}
	}

	keys := make(map[string]struct{}, len(t.Expected))
	for _, row := range t.Expected {
		if !sameColumns(columns, row) {
			return fmt.Errorf("test.expected rows must define the same columns")
		}

		for _, column := range columns {
			if Name(column).IsNotValid() {
				return fmt.Errorf("test.expected column %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", column)
			}
			if _, err := sqlLiteral(row[column]); err != nil {
				return fmt.Errorf("test.expected column %q: %w", column, err)
			}
		}

		key := t.rowKey(row)
		if _, ok := keys[key]; ok {
			return fmt.Errorf("test.expected contains duplicate key values")
		}
		keys[key] = struct{}{}
	}

	return nil
}

func (t Test) validateFields(fields []string) error {
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		if Name(field).IsNotValid() {
			return fmt.Errorf("test field %q is invalid", field)
		}
		if seen[field] {
			return fmt.Errorf("test field %q is repeated", field)
		}
		seen[field] = true
	}
	return nil
}

func (t Test) SQL(database Name) (string, error) {
	if database.IsNotValid() {
		return "", fmt.Errorf("database.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", database.ToString())
	}
	if err := t.Validate(); err != nil {
		return "", err
	}

	from := database.ToString() + "." + t.Table.ToString()
	filter := ""
	if t.Where.IsNotEmpty() {
		filter = " WHERE (" + t.Where.ToString() + ")"
	}
	if len(t.Unique) > 0 {
		fields := stdstrings.Join(t.Unique, ", ")
		return fmt.Sprintf("SELECT %s, count() FROM %s%s GROUP BY %s HAVING count() > 1 ORDER BY %s", fields, from, filter, fields, fields), nil
	}
	if len(t.Checks) > 0 {
		columns := make([]string, len(t.Checks))
		for i, check := range t.Checks {
			condition, err := check.SQL()
			if err != nil {
				return "", err
			}
			columns[i] = fmt.Sprintf("countIf(%s)", condition)
		}
		return fmt.Sprintf("SELECT %s FROM %s%s", stdstrings.Join(columns, ", "), from, filter), nil
	}

	condition := t.where()
	if t.Where.IsNotEmpty() {
		condition = "(" + t.Where.ToString() + ") AND (" + condition + ")"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", stdstrings.Join(t.columns(), ", "), from, condition), nil
}

func (t Test) columns() []string {
	if len(t.Expected) == 0 {
		return nil
	}

	columns := make([]string, 0, len(t.Expected[0]))
	for column := range t.Expected[0] {
		columns = append(columns, column)
	}
	sort.Strings(columns)

	return columns
}

func (t Test) where() string {
	if t.hasNullKey() {
		return t.nullableKeyWhere()
	}

	if len(t.Key) == 1 {
		values := make([]string, 0, len(t.Expected))
		for _, row := range t.Expected {
			value, _ := sqlLiteral(row[t.Key[0]])
			values = append(values, value)
		}
		return t.Key[0] + " IN (" + stdstrings.Join(values, ", ") + ")"
	}

	keys := make([]string, 0, len(t.Expected))
	for _, row := range t.Expected {
		values := make([]string, 0, len(t.Key))
		for _, key := range t.Key {
			value, _ := sqlLiteral(row[key])
			values = append(values, value)
		}
		keys = append(keys, "("+stdstrings.Join(values, ", ")+")")
	}

	return "(" + stdstrings.Join(t.Key, ", ") + ") IN (" + stdstrings.Join(keys, ", ") + ")"
}

func (t Test) hasNullKey() bool {
	for _, row := range t.Expected {
		for _, key := range t.Key {
			if row[key] == nil {
				return true
			}
		}
	}

	return false
}

func (t Test) nullableKeyWhere() string {
	rows := make([]string, 0, len(t.Expected))
	for _, row := range t.Expected {
		conditions := make([]string, 0, len(t.Key))
		for _, key := range t.Key {
			if row[key] == nil {
				conditions = append(conditions, key+" IS NULL")
				continue
			}

			value, _ := sqlLiteral(row[key])
			conditions = append(conditions, key+" = "+value)
		}
		rows = append(rows, "("+stdstrings.Join(conditions, " AND ")+")")
	}

	return "(" + stdstrings.Join(rows, " OR ") + ")"
}

func (t Test) rowKey(row map[string]any) string {
	values := make([]string, 0, len(t.Key))
	for _, key := range t.Key {
		value := "NULL"
		if row[key] != nil {
			value = strconv.Quote(fmt.Sprint(row[key]))
		}
		values = append(values, value)
	}

	return stdstrings.Join(values, ",")
}

func sameColumns(columns []string, row map[string]any) bool {
	if len(columns) != len(row) {
		return false
	}
	for _, column := range columns {
		if _, ok := row[column]; !ok {
			return false
		}
	}

	return true
}

func sqlLiteral(value any) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	switch value := value.(type) {
	case string:
		return "'" + stdstrings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value) + "'", nil
	case bool:
		if value {
			return "true", nil
		}
		return "false", nil
	case time.Time:
		return sqlLiteral(value.Format(time.RFC3339Nano))
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return "", fmt.Errorf("unsupported value %v", value)
		}
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return "", fmt.Errorf("unsupported value %v", value)
		}
	}

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%v", value), nil
	}

	return "", fmt.Errorf("unsupported value type %T", value)
}
