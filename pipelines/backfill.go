package pipelines

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/strings"
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
	b.Statement.WriteString(b.Parent.View.To.Suffix(b.Parent.Config.Suffix).ToString())

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

func (b Backfill) SQL() string {
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

	if b.Parent.View.Name.IsNotValid() {
		return fmt.Errorf("view.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", b.Parent.View.Name.ToString())
	}

	if b.Parent.View.To.IsNotValid() {
		return fmt.Errorf("view.to %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", b.Parent.View.To.ToString())
	}

	if b.Parent.View.To.Suffix(b.Parent.Config.Suffix).IsNotValid() {
		return fmt.Errorf("view.to %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", b.Parent.View.To.Suffix(b.Parent.Config.Suffix).ToString())
	}

	if b.Parent.View.Query.IsEmpty() {
		return fmt.Errorf("view.query is required")
	}

	return nil
}
