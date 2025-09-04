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
	Backfill Backfill `yaml:"-"`
	Config   Config   `yaml:"-"`
	Database Database `yaml:"database"`
	Table    Table    `yaml:"table"`
	View     View     `yaml:"view"`
}

func (p *Pipelines) Reset() {
	p.Backfill = Backfill{}
	p.Database = Database{}
	p.Table = Table{}
	p.View = View{}
}

func (p *Pipelines) Load() error {
	f, err := os.ReadFile(p.Config.Pipe)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(f, p)
}

func (p *Pipelines) SetParents() {
	p.Backfill.Parent = p
	p.Table.Parent = p
	p.View.Parent = p
}
