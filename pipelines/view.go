package pipelines

import (
	"fmt"
	"reflect"

	"github.com/nicola-strappazzon/dacfy/pipelines/columns"
	"github.com/nicola-strappazzon/dacfy/strings"
)

type View struct {
	Columns      columns.Map     `yaml:"columns"`
	Delete       bool            `yaml:"delete"`
	Engine       Engine          `yaml:"engine"`
	Materialized bool            `yaml:"materialized"`
	Name         Name            `yaml:"name"`
	OrderBy      columns.Array   `yaml:"order_by"`
	PartitionBy  columns.Array   `yaml:"partition_by"`
	Populate     Populate        `yaml:"populate"`
	Query        Query           `yaml:"query"`
	Statement    strings.Builder `yaml:"-"`
	To           Name            `yaml:"to"`
	Parent       *Pipelines      `yaml:"-"`
}

func (v View) Drop() View {
	if v.Parent.Database.Name.IsEmpty() {
		return v
	}

	if v.Name.IsEmpty() {
		return v
	}

	if !v.Delete {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("DROP VIEW IF EXISTS ")
	v.Statement.WriteString(v.Parent.Database.Name.ToString())
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name.ToString())

	return v
}

func (v View) Create() View {
	if v.Parent.Database.Name.IsEmpty() {
		return v
	}

	if v.Name.IsEmpty() {
		return v
	}

	if v.Parent.View.Query.IsEmpty() {
		return v
	}

	v.Statement = strings.Builder{}
	v.Statement.WriteString("CREATE ")

	if v.Materialized {
		v.Statement.WriteString("MATERIALIZED ")
	}

	v.Statement.WriteString("VIEW IF NOT EXISTS ")
	v.Statement.WriteString(v.Parent.Database.Name.ToString())
	v.Statement.WriteString(".")
	v.Statement.WriteString(v.Name.ToString())

	if v.Materialized {
		if v.Populate.IsBackFill() {
			v.Statement.WriteString(" TO ")
			v.Statement.WriteString(v.Parent.Database.Name.ToString())
			v.Statement.WriteString(".")
			v.Statement.WriteString(v.To.ToString())
		}

		if v.Populate.IsNotBackFill() {
			if v.Engine.IsNotEmpty() {
				v.Statement.WriteString(" ENGINE=")
				v.Statement.WriteString(v.Engine.ToString())
			}

			if v.PartitionBy.IsNotEmpty() {
				v.Statement.WriteString(" PARTITION BY (")
				v.Statement.WriteString(v.PartitionBy.Join())
				v.Statement.WriteString(")")
			}

			if v.OrderBy.IsNotEmpty() {
				v.Statement.WriteString(" ORDER BY (")
				v.Statement.WriteString(v.OrderBy.Join())
				v.Statement.WriteString(")")
			}

			v.Statement.WriteString(" POPULATE")
		}

		if v.Columns.IsNotEmpty() {
			v.Statement.WriteString(" (")
			v.Statement.WriteString(v.Columns.JoinWithTypes())
			v.Statement.WriteString(")")
		}
	}

	v.Statement.WriteString(" AS ")
	v.Statement.WriteString(v.Query.Minify())

	return v
}

func (v View) DML() string {
	return v.Statement.String()
}

func (v View) Validate() error {
	if reflect.DeepEqual(v, View{Parent: v.Parent}) {
		return nil
	}

	if v.Name.IsEmpty() {
		return fmt.Errorf("view.name is required")
	}

	if v.Name.IsNotValid() {
		return fmt.Errorf("view.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", v.Name.ToString())
	}

	if v.Name.IsEmpty() && v.Delete {
		return fmt.Errorf("view.name is required")
	}

	if v.Query.IsEmpty() {
		return fmt.Errorf("view.query is required")
	}

	if cols, err := v.PartitionBy.NotIn(v.Parent.Table.Columns.ToArray()); err {
		return fmt.Errorf("field(s) %v in view.partition_by not found in columns for view %s", cols, v.Name.ToString())
	}

	if cols, err := v.OrderBy.NotIn(v.Parent.Table.Columns.ToArray()); err {
		return fmt.Errorf("field(s) %v in view.order_by not found in columns for view %s", cols, v.Name.ToString())
	}

	// Validate definition settings:
	// =============================
	// | Case | Materialized | Populate | To    | Engine | PartitionBy | OrderBy | Columns |
	// |------|--------------|----------|-------|--------|-------------|---------|---------|
	// | 1    | false        | false    | false | false  | false       | false   | false   |
	// | 2    | true         | false    | false | true   | true        | true    | false   |
	// | 3    | true         | true     | true  | false  | false       | false   | false   |
	//
	// Case 1:
	if v.IsValidView() {
		return nil
	}

	// Case 2:
	if v.IsValidViewMaterialized() {
		return nil
	}

	// Case 3:
	if v.IsValidViewMaterializedPopulateBackFill() {
		return nil
	}

	return fmt.Errorf("invalid view combination")
}

func (v View) IsValidView() bool {
	return !v.Materialized &&
		v.Populate.IsEmpty() &&
		v.To.IsEmpty() &&
		v.Engine.IsEmpty() &&
		v.PartitionBy.IsEmpty() &&
		v.OrderBy.IsEmpty() &&
		v.Columns.IsEmpty()
}

func (v View) IsValidViewMaterialized() bool {
	return v.Materialized &&
		v.Populate.IsEmpty() &&
		v.To.IsEmpty() &&
		v.Engine.IsNotEmpty() &&
		v.PartitionBy.IsNotEmpty() &&
		v.OrderBy.IsNotEmpty() &&
		v.Columns.IsEmpty()
}

func (v View) IsValidViewMaterializedPopulateBackFill() bool {
	return v.Materialized &&
		v.Populate.IsNotEmpty() &&
		v.To.IsNotEmpty() &&
		v.Engine.IsEmpty() &&
		v.PartitionBy.IsEmpty() &&
		v.OrderBy.IsEmpty() &&
		v.Columns.IsEmpty()
}
