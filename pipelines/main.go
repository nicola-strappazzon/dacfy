package pipelines

import (
	"os"

	"github.com/goccy/go-yaml"
)

var instance *Pipelines

func Instance() *Pipelines {
	if instance == nil {
		instance = &Pipelines{}
	}

	return instance
}

type Pipelines struct {
	Config   Config   `yaml:"-"`
	Database Database `yaml:"database"`
	Table    Table    `yaml:"table"`
	View     View     `yaml:"view"`
	Backfill Backfill `yaml:"-"`
}

func (p *Pipelines) Load(in string) error {
	yamlFile, err := os.ReadFile(in)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(yamlFile, p)
}

func (p *Pipelines) SetParents() {
	p.Table.Parent = p
	p.View.Parent = p
	p.Backfill.Parent = p
}
