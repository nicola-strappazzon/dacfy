package pipelines

import (
	"fmt"
	"regexp"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Table struct {
	Columns     Columns         `yaml:"columns"`
	Delete      bool            `yaml:"delete"`
	Engine      string          `yaml:"engine"`
	Name        string          `yaml:"name"`
	OrderBy     []string        `yaml:"order_by"`
	PartitionBy []string        `yaml:"partition_by"`
	PrimaryKey  []string        `yaml:"primary_key"`
	Query       Query           `yaml:"query"`
	Settings    []string        `yaml:"settings"`
	Statement   strings.Builder `yaml:"-"`
	TTL         string          `yaml:"ttl"`
}

func (t Table) Create() Table {
	if strings.IsEmpty(t.Name) {
		return t
	}

	t.Statement = strings.Builder{}
	t.Statement.WriteString("CREATE TABLE IF NOT EXISTS ")
	t.Statement.WriteString(instance.Database.Name)
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name)
	t.Statement.WriteString(" (")
	t.Statement.WriteString(t.Columns.WithTypes())
	t.Statement.WriteString(") ")

	if strings.IsNotEmpty(t.Engine) {
		t.Statement.WriteString("ENGINE=")
		t.Statement.WriteString(t.Engine)
		t.Statement.WriteString(" ")
	}

	if len(t.PartitionBy) > 0 {
		t.Statement.WriteString("PARTITION BY (")
		t.Statement.WriteString(strings.Join(t.PartitionBy))
		t.Statement.WriteString(") ")
	}

	if len(t.PrimaryKey) > 0 {
		t.Statement.WriteString("PRIMARY KEY (")
		t.Statement.WriteString(strings.Join(t.PrimaryKey))
		t.Statement.WriteString(") ")
	}

	if len(t.OrderBy) > 0 {
		t.Statement.WriteString("ORDER BY (")
		t.Statement.WriteString(strings.Join(t.OrderBy))
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
	t.Statement = strings.Builder{}
	t.Statement.WriteString("DROP TABLE IF EXISTS ")
	t.Statement.WriteString(instance.Database.Name)
	t.Statement.WriteString(".")
	t.Statement.WriteString(t.Name)

	return t
}

func (t Table) DML() string {
	return t.Statement.String()
}

func (t Table) Validate() error {
	var re = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{1,254}$`)

	if strings.IsEmpty(t.Name) && !t.Delete {
		return fmt.Errorf("table.name is required unless delete is true")
	}

	if !re.MatchString(t.Name) {
		return fmt.Errorf("table.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", t.Name)
	}

	if len(t.Columns) == 0 {
		return fmt.Errorf("table.columns must be defined for table %q", t.Name)
	}

	if strings.IsEmpty(t.Engine) {
		return fmt.Errorf("table.engine is required for table %q", t.Name)
	}

	if err := t.ColumnExist("partition_by", t.PartitionBy); err != nil {
		return err
	}

	if err := t.ColumnExist("primary_key", t.PrimaryKey); err != nil {
		return err
	}

	if err := t.ColumnExist("order_by", t.OrderBy); err != nil {
		return err
	}

	for _, s := range t.Settings {
		if !strings.Contains(s, "=") {
			return fmt.Errorf("invalid setting %q in table %q; expected format key=value", s, t.Name)
		}
	}

	return nil
}

func (t Table) ColumnExist(fieldName string, values []string) error {
	columnMap := map[string]bool{}

	for _, col := range t.Columns {
		columnMap[col.Name] = true
	}

	for _, v := range values {
		if !columnMap[v] {
			return fmt.Errorf("field %q in %s not found in columns for table %q", v, fieldName, t.Name)
		}
	}
	return nil
}
