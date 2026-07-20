package pipelines

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/strings"
)

type UserGrant struct {
	Privilege string `yaml:"privilege"`
	On        string `yaml:"on"`
}

type User struct {
	Cluster   Name            `yaml:"cluster"`
	Delete    bool            `yaml:"delete"`
	Grants    []UserGrant     `yaml:"grants"`
	Name      Name            `yaml:"name"`
	Password  string          `yaml:"password"`
	Statement strings.Builder `yaml:"-"`
}

func (u User) IsEmpty() bool {
	return u.Name.IsEmpty()
}

func (u User) IsNotEmpty() bool {
	return !u.IsEmpty()
}

func (u User) Create() User {
	if u.Name.IsNotEmpty() {
		u.Statement = strings.Builder{}
		u.Statement.WriteString("CREATE USER IF NOT EXISTS ")
		u.Statement.WriteString(u.Name.ToString())

		if u.Cluster.IsNotEmpty() {
			u.Statement.WriteString(" ON CLUSTER ")
			u.Statement.WriteString(u.Cluster.ToString())
		}

		if strings.IsNotEmpty(u.Password) {
			u.Statement.WriteString(" IDENTIFIED BY '")
			u.Statement.WriteString(u.Password)
			u.Statement.WriteString("'")
		}
	}

	return u
}

func (u User) Grant(g UserGrant) User {
	if u.Name.IsNotEmpty() {
		u.Statement = strings.Builder{}
		u.Statement.WriteString("GRANT ")

		if u.Cluster.IsNotEmpty() {
			u.Statement.WriteString("ON CLUSTER ")
			u.Statement.WriteString(u.Cluster.ToString())
			u.Statement.WriteString(" ")
		}

		u.Statement.WriteString(g.Privilege)
		u.Statement.WriteString(" ON ")
		u.Statement.WriteString(g.On)
		u.Statement.WriteString(" TO ")
		u.Statement.WriteString(u.Name.ToString())
	}

	return u
}

func (u User) Drop() User {
	if u.Name.IsNotEmpty() {
		u.Statement = strings.Builder{}
		u.Statement.WriteString("DROP USER IF EXISTS ")
		u.Statement.WriteString(u.Name.ToString())

		if u.Cluster.IsNotEmpty() {
			u.Statement.WriteString(" ON CLUSTER ")
			u.Statement.WriteString(u.Cluster.ToString())
		}
	}

	return u
}

func (u User) SQL() string {
	return u.Statement.String()
}

func (u User) Validate() error {
	if u.IsEmpty() {
		return nil
	}

	if u.Name.IsNotValid() {
		return fmt.Errorf("user.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", u.Name.ToString())
	}

	if u.Cluster.IsNotEmpty() && u.Cluster.IsNotValid() {
		return fmt.Errorf("user.cluster %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", u.Cluster.ToString())
	}

	for _, g := range u.Grants {
		if strings.IsEmpty(g.Privilege) {
			return fmt.Errorf("user.grants.privilege is required")
		}

		if strings.IsEmpty(g.On) {
			return fmt.Errorf("user.grants.on is required")
		}
	}

	return nil
}
