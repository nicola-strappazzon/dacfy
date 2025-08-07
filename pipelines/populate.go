package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

func (p Pipelines) PopulateTableName() string {
	if strings.IsNotEmpty(p.Table.Name) {
		return p.Table.Name
	}

	if strings.IsNotEmpty(p.View.To) {
		return p.View.To
	}

	return ""
}

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

	if strings.IsEmpty(p.Table.Name) && strings.IsEmpty(p.View.To) {
		return p
	}

	p.Statement = strings.Builder{}
	p.Statement.WriteString("INSERT INTO ")
	p.Statement.WriteString(p.Database.Name)
	p.Statement.WriteString(".")

	if strings.IsNotEmpty(p.View.To) {
		p.Statement.WriteString(p.View.To)
	} else if strings.IsNotEmpty(p.Table.Name) {
		p.Statement.WriteString(p.Table.Name)
	}

	if strings.IsNotEmpty(p.View.Columns.WithTypes()) {
		p.Statement.WriteString(" (")
		p.Statement.WriteString(p.View.Columns.WithoutTypes())
		p.Statement.WriteString(") ")
	} else if strings.IsNotEmpty(p.Table.Columns.WithTypes()) {
		p.Statement.WriteString(" (")
		p.Statement.WriteString(p.Table.Columns.WithoutTypes())
		p.Statement.WriteString(") ")
	}

	p.Statement.WriteString(p.View.Query.Minify())

	return p
}

func (p Pipelines) DML() string {
	return p.Statement.String()
}
