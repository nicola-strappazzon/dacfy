package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type View struct {
	Columns      Columns         `yaml:"columns"`
	Delete       bool            `yaml:"delete"`
	Engine       string          `yaml:"engine"`
	Materialized bool            `yaml:"materialized"`
	Name         string          `yaml:"name"`
	OrderBy      []string        `yaml:"order_by"`
	PartitionBy  []string        `yaml:"partition_by"`
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

	if v.IsNormal() {
		return v.DoNormal()
	}

	if v.IsMaterialized() {
		return v.DoMaterialized()
	}

	// IsMaterializedNative --> DoMaterializedNative
	// IsMaterializedQuery  --> ...
	// IsMaterializedChunk  --> ...

	return v
}

func (v View) IsNormal() bool {
	return (v.Materialized == false)
}

func (v View) IsMaterialized() bool {
	return (v.Materialized == true)
}

func (v View) DoNormal() View {
	if strings.IsEmpty(v.Name) {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("CREATE VIEW IF NOT EXISTS ")
	v.Statement.WriteString(instance.Database.Name)
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name)
	v.Statement.WriteString(" (")
	v.Statement.WriteString(instance.View.Columns.WithTypes())
	v.Statement.WriteString(") ")
	v.Statement.WriteString(" AS ")
	v.Statement.WriteString(instance.View.Query.Minify())

	return v
}

func (v View) DoMaterialized() View {
	if strings.IsEmpty(v.Name) {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
	v.Statement.WriteString(instance.Database.Name)
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name)
	v.Statement.WriteString(" ")

	if strings.IsNotEmpty(instance.Table.Name) {
		v.Statement.WriteString("TO ")
		v.Statement.WriteString(instance.Database.Name)
		v.Statement.WriteString(".")
		v.Statement.WriteString(instance.Table.Name)
	}

	if strings.IsNotEmpty(v.To) {
		v.Statement.WriteString("TO ")
		v.Statement.WriteString(instance.Database.Name)
		v.Statement.WriteString(".")
		v.Statement.WriteString(v.To)
	}

	if strings.IsNotEmpty(v.Columns.WithTypes()) {
		v.Statement.WriteString(" (")
		v.Statement.WriteString(v.Columns.WithTypes())
		v.Statement.WriteString(") ")
	}

	v.Statement.WriteString("AS ")
	v.Statement.WriteString(instance.View.Query.Minify())

	return v
}

func (v View) DML() string {
	return v.Statement.String()
}
