package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

func (p Pipelines) Populate() Pipelines {
	if !p.View.Materialized {
		return p
	}

	if p.View.Populate.Skip {
		return p
	}

	if p.View.Populate.Type != PopulateQuery {
		return p
	}

	if strings.IsEmpty(p.Database.Name) {
		return p
	}

	if strings.IsEmpty(p.Table.Name) {
		return p
	}

	p.Statement = strings.Builder{}
	p.Statement.WriteString("INSERT INTO ")
	p.Statement.WriteString(p.Database.Name)
	p.Statement.WriteString(".")
	p.Statement.WriteString(p.Table.Name)
	p.Statement.WriteString(" (")
	p.Statement.WriteString(p.Table.Columns.WithoutTypes())
	p.Statement.WriteString(") ")
	p.Statement.WriteString(p.View.Query.Minify())

	return p
}

func (p Pipelines) DML() string {
	return p.Statement.String()
}
