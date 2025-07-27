package pipelines

import (
	"os"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"

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
	Config    Config          `yaml:"-"`
	Database  Database        `yaml:"database"`
	Statement strings.Builder `yaml:"-"`
	Table     Table           `yaml:"table"`
	View      View            `yaml:"view"`
}

func (p *Pipelines) Load(in string) {
	yamlFile, err := os.ReadFile(in)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, p)
	if err != nil {
		panic(err)
	}
}
