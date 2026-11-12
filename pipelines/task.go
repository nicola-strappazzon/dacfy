package pipelines

import (
	"errors"
	"fmt"
	"regexp"
	stdstrings "strings"
)

// taskName is a task name. It is only used on the command line, so it also
// accepts hyphens.
var taskName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{1,254}$`)

// Task is a named query that the task command runs on demand.
type Task struct {
	Description string `yaml:"description"`
	Name        Name   `yaml:"name"`
	Query       Query  `yaml:"query"`
}

func (t Task) Validate() error {
	if !taskName.MatchString(t.Name.ToString()) {
		return fmt.Errorf("task.name %q is invalid; must start with a letter and contain only letters, digits, underscores or hyphens (max 255 characters)", t.Name.ToString())
	}
	if t.Query.IsEmpty() {
		return fmt.Errorf("task.query is required for task %q", t.Name.ToString())
	}
	return nil
}

// SQL returns the task query with {table} replaced by table, written as
// database.table.
func (t Task) SQL(table string) (string, error) {
	statement := stdstrings.TrimSuffix(t.Query.ToString(), ";")
	if !stdstrings.Contains(statement, tablePlaceholder) {
		return statement, nil
	}
	if table == "" {
		return "", fmt.Errorf("task %q uses %s but table.name is not set", t.Name.ToString(), tablePlaceholder)
	}
	return stdstrings.ReplaceAll(statement, tablePlaceholder, table), nil
}

// Tasks is the tasks list of a manifest.
type Tasks []Task

func (t Tasks) Validate() error {
	if len(t) == 0 {
		return errors.New("no tasks defined")
	}
	seen := make(map[Name]bool, len(t))
	for _, item := range t {
		if err := item.Validate(); err != nil {
			return err
		}
		if seen[item.Name] {
			return fmt.Errorf("task.name %q is repeated", item.Name.ToString())
		}
		seen[item.Name] = true
	}
	return nil
}

// Find returns the task with this name, after it checks the whole list.
func (t Tasks) Find(name string) (Task, error) {
	if err := t.Validate(); err != nil {
		return Task{}, err
	}
	for _, item := range t {
		if item.Name.ToString() == name {
			return item, nil
		}
	}
	return Task{}, fmt.Errorf("task %q not found", name)
}
