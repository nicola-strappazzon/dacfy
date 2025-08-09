package pipelines

import (
	"fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Backfill struct {
	Statement strings.Builder `yaml:"-"`
	Parent    *Pipelines      `yaml:"-"`
}

func (b Backfill) Do() Backfill {
	if !b.Parent.View.Materialized {
		return b
	}

	if b.Parent.View.Populate.IsNotBackFill() {
		return b
	}

	if b.Parent.Database.Name.IsEmpty() {
		return b
	}

	if b.Parent.View.To.IsEmpty() {
		return b
	}

	if b.Parent.View.Query.IsEmpty() {
		return b
	}

	b.Statement = strings.Builder{}
	b.Statement.WriteString("INSERT INTO ")
	b.Statement.WriteString(b.Parent.Database.Name.ToString())
	b.Statement.WriteString(".")
	b.Statement.WriteString(b.Parent.View.To.ToString())

	if b.Parent.View.Columns.IsNotEmpty() {
		b.Statement.WriteString(" (")
		b.Statement.WriteString(b.Parent.View.Columns.JoinWithoutTypes())
		b.Statement.WriteString(") ")
	} else if b.Parent.Table.Columns.IsNotEmpty() {
		b.Statement.WriteString(" (")
		b.Statement.WriteString(b.Parent.Table.Columns.JoinWithoutTypes())
		b.Statement.WriteString(") ")
	}

	b.Statement.WriteString(b.Parent.View.Query.Minify())

	return b
}

func (b Backfill) DML() string {
	return b.Statement.String()
}

func (b Backfill) Validate() error {
	if b.Parent.Database.Name.IsEmpty() {
		return fmt.Errorf("database.name is required")
	}

	if !b.Parent.View.Materialized {
		return fmt.Errorf("view.materialized is required")
	}

	if b.Parent.View.Populate.IsNotBackFill() {
		return fmt.Errorf("view.populate is not 'backfill', maybe it's just a view, check the documentation")
	}

	if b.Parent.View.To.IsNotValid() {
		return fmt.Errorf("view.to is required")
	}

	if b.Parent.View.Columns.IsEmpty() {
		return fmt.Errorf("view.columns must be defined for view %q", b.Parent.View.Name.ToString())
	} else if b.Parent.Table.Columns.IsNotEmpty() {
		return fmt.Errorf("table.columns must be defined for table %q", b.Parent.Table.Name.ToString())
	}

	if b.Parent.View.Query.IsEmpty() {
		return fmt.Errorf("view.query is required")
	}

	return nil
}
