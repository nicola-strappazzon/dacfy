package pipelines

import (
	"fmt"

	"github.com/goccy/go-yaml"
)

// BackfillOptions are the options of a backfill run. They match the flags of
// the backfill command.
type BackfillOptions struct {
	From     string `yaml:"from"`
	To       string `yaml:"to"`
	Chunk    string `yaml:"chunk"`
	Truncate bool   `yaml:"truncate"`
}

// Step is one entry of the pipelines list in a main file. A plain file name
// creates that file. A backfill entry runs a backfill of that file.
type Step struct {
	File     string
	Backfill bool
	Options  BackfillOptions
}

// UnmarshalYAML reads a step from a file name or from a backfill entry.
func (s *Step) UnmarshalYAML(b []byte) error {
	if err := yaml.Unmarshal(b, &s.File); err == nil {
		return nil
	}

	var entry struct {
		File            string `yaml:"backfill"`
		BackfillOptions `yaml:",inline"`
	}

	if err := yaml.UnmarshalWithOptions(b, &entry, yaml.DisallowUnknownField()); err != nil {
		return fmt.Errorf("pipelines entry must be a file name or a backfill entry: %w", err)
	}

	if entry.File == "" {
		return fmt.Errorf("pipelines backfill entry requires a file name")
	}

	s.File = entry.File
	s.Backfill = true
	s.Options = entry.BackfillOptions

	return nil
}
