package pipelines

import "fmt"

type MongoDB struct {
	Collection string `yaml:"collection"`
	URI        string `yaml:"uri"`
}

func (m MongoDB) IsEmpty() bool {
	return m.URI == "" && m.Collection == ""
}

func (m MongoDB) Validate() error {
	if m.URI == "" {
		return fmt.Errorf("mongodb.uri is required")
	}

	if m.Collection == "" {
		return fmt.Errorf("mongodb.collection is required")
	}

	return nil
}

func (m MongoDB) EngineString() string {
	return fmt.Sprintf("MongoDB('%s', '%s')", m.URI, m.Collection)
}
