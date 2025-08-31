package pipelines_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestDatabase_Create(t *testing.T) {
	t.Run("with name", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("foo"),
		}

		assert.Equal(t, "CREATE DATABASE IF NOT EXISTS foo", d.Create().SQL())
		assert.Empty(t, d.SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		d := pipelines.Database{}

		assert.Empty(t, d.Create().SQL())
	})
}

func TestDatabase_Drop(t *testing.T) {
	t.Run("with name", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("foo"),
		}

		assert.Equal(t, "DROP DATABASE IF EXISTS foo", d.Drop().SQL())
		assert.Empty(t, d.SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		d := pipelines.Database{}

		assert.Empty(t, d.Drop().SQL())
	})
}

func TestDatabase_Use(t *testing.T) {
	t.Run("with name", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("foo"),
		}

		assert.Equal(t, "USE foo", d.Use().SQL())
		assert.Empty(t, d.SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		d := pipelines.Database{}

		assert.Empty(t, d.Use().SQL())
	})
}

func TestDatabase_DML(t *testing.T) {
	t.Run("returns underlying builder string", func(t *testing.T) {
		d := pipelines.Database{Name: pipelines.Name("foo")}

		assert.Equal(t, "CREATE DATABASE IF NOT EXISTS foo", d.Create().SQL())
	})
}

func TestDatabase_Validate(t *testing.T) {
	t.Run("empty name -> required error", func(t *testing.T) {
		d := pipelines.Database{}
		e := d.Validate()
		assert.Error(t, e)
		assert.Equal(t, "database.name is required", e.Error())
	})

	t.Run("delete true + empty name -> still required error (first check wins)", func(t *testing.T) {
		d := pipelines.Database{
			Delete: true,
		}
		e := d.Validate()
		assert.Error(t, e)
		assert.Equal(t, "database.name is required", e.Error())
	})

	t.Run("invalid: starts with digit", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("1foo"),
		}
		e := d.Validate()
		assert.Error(t, e)
		assert.Equal(t, `database.name "1foo" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, e.Error())
	})

	t.Run("invalid: contains hyphen", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("db-name"),
		}
		e := d.Validate()
		assert.Error(t, e)
		assert.Equal(t, `database.name "db-name" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, e.Error())
	})

	t.Run("invalid: length 1 (regex requires a minimum 2)", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("a"),
		}
		e := d.Validate()
		assert.Error(t, e)
		assert.Equal(t, `database.name "a" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, e.Error())
	})

	t.Run("valid: minimal length 2", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("foo"),
		}
		assert.NoError(t, d.Validate())
	})

	t.Run("valid: with underscore and digits", func(t *testing.T) {
		d := pipelines.Database{
			Name: pipelines.Name("foo_1"),
		}
		assert.NoError(t, d.Validate())
	})

	t.Run("valid: max length 255", func(t *testing.T) {
		name := "d" + strings.Repeat("a", 254) // len=255
		d := pipelines.Database{
			Name: pipelines.Name(name),
		}
		assert.NoError(t, d.Validate())
	})

	t.Run("invalid: length 256", func(t *testing.T) {
		n := "d" + strings.Repeat("a", 255) // len=256
		d := pipelines.Database{
			Name: pipelines.Name(n),
		}
		e := d.Validate()
		s := fmt.Sprintf(`database.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, n)
		assert.Error(t, e)
		assert.Equal(t, s, e.Error())
	})

	t.Run("delete true + valid name -> ok", func(t *testing.T) {
		d := pipelines.Database{
			Name:   pipelines.Name("db"),
			Delete: true,
		}
		assert.NoError(t, d.Validate())
	})
}
