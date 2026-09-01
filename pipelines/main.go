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
	Backfill  Backfill `yaml:"-"`
	Config    Config   `yaml:"-"`
	Database  Database `yaml:"database"`
	Pipelines []string `yaml:"pipelines"`
	Table     Table    `yaml:"table"`
	User      User     `yaml:"user"`
	View      View     `yaml:"view"`
}

func (p *Pipelines) Reset() {
	p.Backfill = Backfill{}
	p.Database = Database{}
	p.Pipelines = nil
	p.Table = Table{}
	p.User = User{}
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

	if err := yaml.Unmarshal(file.ReadExpandEnv(f), p); err != nil {
		return err
	}

	if p.Config.NoCluster {
		p.Database.Cluster = ""
		p.User.Cluster = ""
		p.Database.Replicated = DatabaseReplicated{}
		p.Table.Engine = p.Table.Engine.WithoutReplicated()
		p.View.Engine = p.View.Engine.WithoutReplicated()
	}

	return nil
}

func (p *Pipelines) SetParents() {
	p.Backfill.Parent = p
	p.Table.Parent = p
	p.View.Parent = p
}

func (p *Pipelines) LoadFile(path string) error {
	p.Reset()
	p.Config.Pipe = path

	if err := p.Load(); err != nil {
		return err
	}

	p.SetParents()

	return nil
}
