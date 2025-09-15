package pipelines

import (
	"fmt"
	"os"

	"github.com/nicola-strappazzon/dacfy/file"

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

	for _, v := range file.FindEnvVars(f) {
		if _, ok := os.LookupEnv(v); !ok {
			return fmt.Errorf("Environment variable %q referenced in configuration file is not defined.", v)
		}
	}

	return yaml.Unmarshal(file.ReadExpandEnv(f), p)
}

func (p *Pipelines) SetParents() {
	p.Backfill.Parent = p
	p.Table.Parent = p
	p.View.Parent = p
}
