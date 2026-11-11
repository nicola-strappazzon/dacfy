package pipelines

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/strings"
)

// Index is a data skipping index that dacfy adds to a table after it creates
// the table.
type Index struct {
	Expression  string `yaml:"expression"`
	Granularity uint   `yaml:"granularity"`
	Name        Name   `yaml:"name"`
	Type        string `yaml:"type"`
}

func (i Index) Validate() error {
	if i.Name.IsNotValid() {
		return fmt.Errorf("index.name %q is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)", i.Name.ToString())
	}
	if strings.IsEmpty(i.Expression) {
		return fmt.Errorf("index.expression is required for index %q", i.Name.ToString())
	}
	if strings.IsEmpty(i.Type) {
		return fmt.Errorf("index.type is required for index %q", i.Name.ToString())
	}
	return nil
}

// SQL returns the statement that adds the index to the table, written as
// database.table.
func (i Index) SQL(table string) string {
	statement := fmt.Sprintf("ALTER TABLE %s ADD INDEX IF NOT EXISTS %s %s TYPE %s", table, i.Name, strings.TrimSpace(i.Expression), strings.TrimSpace(i.Type))
	if i.Granularity > 0 {
		statement += fmt.Sprintf(" GRANULARITY %d", i.Granularity)
	}
	return statement
}
