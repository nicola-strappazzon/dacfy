package pipelines

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/nicola-strappazzon/dacfy/strings"
)

type Database struct {
	Cluster    Name               `yaml:"cluster"`
	Delete     bool               `yaml:"delete"`
	Name       Name               `yaml:"name"`
	Replicated DatabaseReplicated `yaml:"replicated"`
	Statement  strings.Builder    `yaml:"-"`
}

type DatabaseReplicated struct {
	Path    string `yaml:"path"`
	Replica string `yaml:"replica"`
}

func (r DatabaseReplicated) IsEmpty() bool {
	return strings.IsEmpty(r.Path) && strings.IsEmpty(r.Replica)
}

func (r DatabaseReplicated) IsNotEmpty() bool {
	return !r.IsEmpty()
}

func (d Database) Create() Database {
	if d.Name.IsNotEmpty() {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("CREATE DATABASE IF NOT EXISTS ")
		d.Statement.WriteString(d.Name.ToString())

		if d.Cluster.IsNotEmpty() {
			d.Statement.WriteString(" ON CLUSTER ")
			d.Statement.WriteString(d.Cluster.ToString())
		}

		if d.Replicated.IsNotEmpty() {
			d.Statement.WriteString(" ENGINE = Replicated('")
			d.Statement.WriteString(d.Replicated.Path)
			d.Statement.WriteString("', '")
			d.Statement.WriteString(d.Replicated.Replica)
			d.Statement.WriteString("')")
		}
	}

	return d
}

func (d Database) Drop() Database {
	if d.Name.IsNotEmpty() {
		d.Statement = strings.Builder{}
		d.Statement.WriteString("DROP DATABASE IF EXISTS ")
		d.Statement.WriteString(d.Name.ToString())

		if d.Cluster.IsNotEmpty() {
			d.Statement.WriteString(" ON CLUSTER ")
			d.Statement.WriteString(d.Cluster.ToString())
		}
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

func (d Database) SQL() string {
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

	if d.Cluster.IsNotEmpty() && d.Cluster.IsNotValid() {
		return fmt.Errorf("database.cluster %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", d.Cluster.ToString())
	}

	if d.Replicated.IsNotEmpty() {
		if strings.IsEmpty(d.Replicated.Path) {
			return fmt.Errorf("database.replicated.path is required")
		}

		if strings.IsEmpty(d.Replicated.Replica) {
			return fmt.Errorf("database.replicated.replica is required")
		}
	}

	return nil
}
