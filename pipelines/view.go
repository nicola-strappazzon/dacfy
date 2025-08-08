package pipelines

import (
	"reflect"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type View struct {
	Columns      ColumnsMap      `yaml:"columns"`
	Delete       bool            `yaml:"delete"`
	Engine       string          `yaml:"engine"`
	Materialized bool            `yaml:"materialized"`
	Name         string          `yaml:"name"`
	OrderBy      ColumnsArray    `yaml:"order_by"`
	PartitionBy  ColumnsArray    `yaml:"partition_by"`
	Populate     Populate        `yaml:"populate"`
	Query        Query           `yaml:"query"`
	Statement    strings.Builder `yaml:"-"`
	To           string          `yaml:"to"`
}

func (v View) Drop() View {
	if strings.IsEmpty(v.Name) {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("DROP VIEW IF EXISTS ")
	v.Statement.WriteString(instance.Database.Name)
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name)

	return v
}

func (v View) Create() View {
	if strings.IsEmpty(instance.Database.Name) {
		return v
	}

	if strings.IsEmpty(v.Name) {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("CREATE ")

	if v.Materialized {
		v.Statement.WriteString("MATERIALIZED ")
	}

	v.Statement.WriteString("VIEW IF NOT EXISTS ")
	v.Statement.WriteString(instance.Database.Name)
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name)

	v.Statement.WriteString(" TO ")
	v.Statement.WriteString(instance.Database.Name)
	v.Statement.WriteString(".")

	if strings.IsNotEmpty(instance.Table.Name) {
		v.Statement.WriteString(instance.Table.Name)
	}

	if strings.IsNotEmpty(v.To) {
		v.Statement.WriteString(v.To)
	}

	if v.Materialized && strings.IsNotEmpty(v.Columns.WithTypes()) {
		v.Statement.WriteString(" (")
		v.Statement.WriteString(v.Columns.WithTypes())
		v.Statement.WriteString(")")
	}

	if !v.Materialized && strings.IsNotEmpty(instance.View.Columns.WithTypes()) {
		v.Statement.WriteString(" (")
		v.Statement.WriteString(instance.View.Columns.WithTypes())
		v.Statement.WriteString(")")
	}

	v.Statement.WriteString(" AS ")
	v.Statement.WriteString(instance.View.Query.Minify())

	return v
}

func (v View) DML() string {
	return v.Statement.String()
}

func (v View) Validate() error {
	if reflect.DeepEqual(v, View{}) {
		return nil
	}
	return nil
}
