package pipelines

import (
	"fmt"
	"reflect"

	"github.com/nicola-strappazzon/dacfy/pipelines/columns"
	"github.com/nicola-strappazzon/dacfy/strings"
)

type Table struct {
	Columns     columns.Map     `yaml:"columns"`
	Delete      bool            `yaml:"delete"`
	Engine      Engine          `yaml:"engine"`
	Name        Name            `yaml:"name"`
	OrderBy     columns.Array   `yaml:"order_by"`
	Parent      *Pipelines      `yaml:"-"`
	PartitionBy columns.Array   `yaml:"partition_by"`
	PrimaryKey  columns.Array   `yaml:"primary_key"`
	Query       Query           `yaml:"query"`
	Settings    []string        `yaml:"settings"`
	Statement   strings.Builder `yaml:"-"`
	TTL         string          `yaml:"ttl"`
}

func (t Table) SetName(in string) Table {
	t.Name = Name(in)

	return t
}

func (t Table) SetSuffix(in string) Table {
	t.Name = t.Name.Suffix(in)

	return t
}

func (t Table) Create() Table {
	if t.Parent.Database.Name.IsEmpty() {
		return t
	}

	if t.Name.IsEmpty() {
		return t
	}

	t.Statement = strings.Builder{}
	t.Statement.WriteString("CREATE TABLE IF NOT EXISTS ")
	t.Statement.WriteString(t.Parent.Database.Name.ToString())
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name.ToString())
	t.Statement.WriteString(" (")
	t.Statement.WriteString(t.Columns.JoinWithTypes())
	t.Statement.WriteString(") ")

	if t.Engine.IsNotEmpty() {
		t.Statement.WriteString("ENGINE=")
		t.Statement.WriteString(t.Engine.ToString())
		t.Statement.WriteString(" ")
	}

	if t.PartitionBy.IsNotEmpty() {
		t.Statement.WriteString("PARTITION BY (")
		t.Statement.WriteString(t.PartitionBy.Join())
		t.Statement.WriteString(") ")
	}

	if t.PrimaryKey.IsNotEmpty() {
		t.Statement.WriteString("PRIMARY KEY (")
		t.Statement.WriteString(t.PrimaryKey.Join())
		t.Statement.WriteString(") ")
	}

	if t.OrderBy.IsNotEmpty() {
		t.Statement.WriteString("ORDER BY (")
		t.Statement.WriteString(t.OrderBy.Join())
		t.Statement.WriteString(") ")
	}

	if strings.IsNotEmpty(t.TTL) {
		t.Statement.WriteString("TTL ")
		t.Statement.WriteString(t.TTL)
		t.Statement.WriteString(" ")
	}

	if len(t.Settings) > 0 {
		t.Statement.WriteString("SETTINGS ")
		t.Statement.WriteString(strings.Join(t.Settings))
	}

	return t
}

func (t Table) Drop() Table {
	if t.Parent.Database.Name.IsEmpty() {
		return t
	}

	if t.Name.IsEmpty() {
		return t
	}

	t.Statement = strings.Builder{}
	t.Statement.WriteString("DROP TABLE IF EXISTS ")
	t.Statement.WriteString(t.Parent.Database.Name.ToString())
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name.ToString())

	return t
}

func (t Table) Truncate() Table {
	if t.Parent.Database.Name.IsEmpty() {
		return t
	}

	if t.Name.IsEmpty() {
		return t
	}

	t.Statement = strings.Builder{}
	t.Statement.WriteString("TRUNCATE TABLE ")
	t.Statement.WriteString(t.Parent.Database.Name.ToString())
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name.ToString())

	return t
}

func (t Table) Rename(in string) Table {
	if t.Name.IsEmpty() {
		return t
	}

	t.Statement = strings.Builder{}
	t.Statement.WriteString("RENAME TABLE ")
	t.Statement.WriteString(t.Parent.Database.Name.ToString())
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name.ToString())
	t.Statement.WriteString(" TO ")
	t.Statement.WriteString(t.Parent.Database.Name.ToString())
	t.Statement.WriteString(".")
	t.Statement.WriteString(in)

	return t
}

func (t Table) SQL() string {
	return t.Statement.String()
}

func (t Table) Validate() error {
	if reflect.DeepEqual(t, Table{Parent: t.Parent}) {
		return nil
	}

	if t.Name.IsEmpty() {
		return fmt.Errorf("table.name is required")
	}

	if t.Name.IsNotValid() {
		return fmt.Errorf("table.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", t.Name.ToString())
	}

	if t.Columns.IsEmpty() {
		return fmt.Errorf("table.columns must be defined for table %q", t.Name.ToString())
	}

	if t.Engine.IsEmpty() {
		return fmt.Errorf("table.engine is required for table %q", t.Name.ToString())
	}

	if cols, ok := t.PartitionBy.NotIn(t.Columns.ToArray()); ok {
		return fmt.Errorf("field(s) %v in table.partition_by not found in columns for table %s", cols, t.Name.ToString())
	}

	if cols, ok := t.PrimaryKey.NotIn(t.Columns.ToArray()); ok {
		return fmt.Errorf("field(s) %v in table.primary_key not found in columns for table %s", cols, t.Name.ToString())
	}

	if cols, ok := t.OrderBy.NotIn(t.Columns.ToArray()); ok {
		return fmt.Errorf("field(s) %v in table.order_by not found in columns for table %s", cols, t.Name.ToString())
	}

	for _, s := range t.Settings {
		if !strings.Contains(s, "=") {
			return fmt.Errorf("invalid setting %q in table %q; expected format key=value", s, t.Name.ToString())
		}
	}

	return nil
}
