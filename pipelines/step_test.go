package pipelines_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadSteps(t *testing.T, yaml string) (*pipelines.Pipelines, error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "main.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	p := &pipelines.Pipelines{}
	return p, p.LoadFile(path)
}

func TestStep_Unmarshal(t *testing.T) {
	p, err := loadSteps(t, `pipelines:
  - table.yaml
  - backfill: table.yaml
    from: "2025-02-03 00:00:00"
    to: "2025-02-05 00:00:00"
    chunk: 1d
    truncate: true
    reverse: true
  - backfill: view.yaml
`)
	require.NoError(t, err)

	assert.Equal(t, []pipelines.Step{
		{File: "table.yaml"},
		{
			File:     "table.yaml",
			Backfill: true,
			Options: pipelines.BackfillOptions{
				From:     "2025-02-03 00:00:00",
				To:       "2025-02-05 00:00:00",
				Chunk:    "1d",
				Truncate: true,
				Reverse:  true,
			},
		},
		{File: "view.yaml", Backfill: true},
	}, p.Pipelines)
}

func TestStep_UnmarshalInvalid(t *testing.T) {
	cases := map[string]string{
		"unknown field": `pipelines:
  - backfill: table.yaml
    chunck: 1d
`,
		"missing file name": `pipelines:
  - from: "2025-02-03 00:00:00"
`,
		"unknown entry": `pipelines:
  - create: table.yaml
`,
		"invalid truncate": `pipelines:
  - backfill: table.yaml
    truncate: maybe
`,
	}

	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := loadSteps(t, yaml)
			assert.Error(t, err)
		})
	}
}
