package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/pipelines/columns"

	"github.com/stretchr/testify/assert"
)

func TestView_Drop_DataBaseNameIsEmpty(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name(""),
		},
		View: pipelines.View{
			Name: pipelines.Name("bar"),
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Empty(t, v.View.Drop().DML())
}

func TestView_Drop_TableNameIsEmpty(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name: pipelines.Name(""),
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Empty(t, v.View.Drop().DML())
}

func TestView_Drop(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name:   pipelines.Name("bar"),
			Delete: true,
		},
	}
	v.SetParents()

	assert.Empty(t, v.View.DML())
	assert.Equal(t, "DROP VIEW IF EXISTS foo.bar", v.View.Drop().DML())
}

func TestView_Create(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name:  pipelines.Name("bar"),
			Query: "SELECT now()",
		},
	}
	v.SetParents()

	assert.True(t, v.View.IsValidView())
	assert.False(t, v.View.IsValidViewMaterialized())
	assert.False(t, v.View.IsValidViewMaterializedPopulateBackFill())
	assert.Equal(t, "CREATE VIEW IF NOT EXISTS foo.bar AS SELECT now()", v.View.Create().DML())
}

func TestView_Create_Materialized(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name:         pipelines.Name("bar"),
			Delete:       true,
			Materialized: true,
			Engine:       "SummingMergeTree",
			PartitionBy:  columns.Array{columns.Name("created_at")},
			OrderBy:      columns.Array{columns.Name("created_at")},
			Query:        "SELECT now() AS created_at",
		},
	}
	v.SetParents()

	assert.False(t, v.View.IsValidView())
	assert.True(t, v.View.IsValidViewMaterialized())
	assert.False(t, v.View.IsValidViewMaterializedPopulateBackFill())
}

func TestView_Create_Materialized_Populate_BackFill(t *testing.T) {
	v := pipelines.Pipelines{
		Database: pipelines.Database{
			Name: pipelines.Name("foo"),
		},
		View: pipelines.View{
			Name:         pipelines.Name("bar"),
			To:           pipelines.Name("baz"),
			Delete:       true,
			Materialized: true,
			Populate:     "backfill",
			Query:        "SELECT now() AS created_at",
		},
	}
	v.SetParents()

	assert.False(t, v.View.IsValidView())
	assert.False(t, v.View.IsValidViewMaterialized())
	assert.True(t, v.View.IsValidViewMaterializedPopulateBackFill())
}
