package pipelines

import (
	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Database struct {
	Name      string          `yaml:"name"`
	Delete    bool            `yaml:"delete"`
	Statement strings.Builder `yaml:"-"`
}

func (d Database) Create() Database {
	if strings.IsNotEmpty(d.Name) {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("CREATE DATABASE IF NOT EXISTS ")
		d.Statement.WriteString(d.Name)
	}

	return d
}

func (d Database) Drop() Database {
	if strings.IsNotEmpty(d.Name) {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("DROP DATABASE IF EXISTS ")
		d.Statement.WriteString(d.Name)
	}

	return d
}

func (d Database) Use() Database {
	if strings.IsNotEmpty(d.Name) {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("USE ")
		d.Statement.WriteString(d.Name)
	}

	return d
}

func (d Database) DML() string {
	return d.Statement.String()
}
