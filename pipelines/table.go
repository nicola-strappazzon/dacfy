package pipelines

import (
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
