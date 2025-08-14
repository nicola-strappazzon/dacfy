package pipelines

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/nicola-strappazzon/dacfy/strings"
)

type Database struct {
	Name      Name            `yaml:"name"`
	Delete    bool            `yaml:"delete"`
	Statement strings.Builder `yaml:"-"`
}

func (d Database) Create() Database {
	if d.Name.IsNotEmpty() {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("CREATE DATABASE IF NOT EXISTS ")
		d.Statement.WriteString(d.Name.ToString())
	}

	return d
}

func (d Database) Drop() Database {
	if d.Name.IsNotEmpty() {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("DROP DATABASE IF EXISTS ")
		d.Statement.WriteString(d.Name.ToString())
	}

	return d
}

func (d Database) Use() Database {
	if d.Name.IsNotEmpty() {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("USE ")
		d.Statement.WriteString(d.Name.ToString())
	}

	return d
}

func (d Database) DML() string {
	return d.Statement.String()
}

func (d Database) Validate() error {
	if reflect.DeepEqual(d, Database{}) {
		return fmt.Errorf("database.name is required")
	}

	if d.Name.IsEmpty() {
		return fmt.Errorf("database.name is required")
	}

	if d.Delete && d.Name.IsEmpty() {
		return errors.New("cannot delete unnamed database")
	}

	if d.Name.IsNotValid() {
		return fmt.Errorf("database.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", d.Name.ToString())
	}

	return nil
}
