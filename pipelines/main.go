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

func (p *Pipelines) Load(in string) error {
	yamlFile, err := os.ReadFile(in)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(yamlFile, p)
}
