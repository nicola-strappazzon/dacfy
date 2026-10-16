package pipelines_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelines_LoadNoCluster(t *testing.T) {
	yaml := `database:
  name: mydb
  cluster: my_cluster
  replicated:
    path: /clickhouse/databases/mydb
    replica: "{replica}"
user:
  name: alice
  cluster: my_cluster
table:
  name: events
  engine: ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
  columns:
    - { name: id, type: UInt64 }
  order_by:
    - id
`
	path := filepath.Join(t.TempDir(), "cl.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	t.Run("keeps cluster and replicated by default", func(t *testing.T) {
		p := &pipelines.Pipelines{}
		require.NoError(t, p.LoadFile(path))

		assert.Equal(t, "my_cluster", p.Database.Cluster.ToString())
		assert.Equal(t, "my_cluster", p.User.Cluster.ToString())
		assert.True(t, p.Database.Replicated.IsNotEmpty())
		assert.Equal(t, "ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')", p.Table.Engine.ToString())
	})

	t.Run("clears cluster and replicated when NoCluster is set", func(t *testing.T) {
		p := &pipelines.Pipelines{}
		p.Config.NoCluster = true
		require.NoError(t, p.LoadFile(path))

		assert.Empty(t, p.Database.Cluster.ToString())
		assert.Empty(t, p.User.Cluster.ToString())
		assert.True(t, p.Database.Replicated.IsEmpty())
		assert.Equal(t, "MergeTree", p.Table.Engine.ToString())
	})
}

func TestPipelines_LoadViewBackfillQuery(t *testing.T) {
	yaml := `database:
  name: analytics
view:
  materialized: true
  populate: backfill
  name: events_mv
  to: events_daily
  query: SELECT id, event_time FROM live_events
  backfill:
    query: SELECT id, event_time FROM historical_events
    where: "event_time >= '{date_from}' AND event_time < '{date_to}'"
`
	path := filepath.Join(t.TempDir(), "backfill.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	p := &pipelines.Pipelines{}
	require.NoError(t, p.LoadFile(path))

	assert.Equal(t, "SELECT id, event_time FROM historical_events", p.View.Backfill.Query.ToString())
	assert.Equal(t, "event_time >= '{date_from}' AND event_time < '{date_to}'", p.View.Backfill.Where.ToString())
}

func TestPipelines_LoadMultilineDescriptions(t *testing.T) {
	yaml := `description: |
  Defines the analytics pipeline.
  Used to document its objects.
table:
  name: events
  description: |
    Stores raw events.
    Retained for 30 days.
view:
  name: events_daily
  description: |
    Aggregates events by day.
    Used by the analytics dashboard.
`
	path := filepath.Join(t.TempDir(), "descriptions.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	p := &pipelines.Pipelines{}
	require.NoError(t, p.LoadFile(path))

	assert.Equal(t, "Defines the analytics pipeline.\nUsed to document its objects.\n", p.Description)
	assert.Equal(t, "Stores raw events.\nRetained for 30 days.\n", p.Table.Description)
	assert.Equal(t, "Aggregates events by day.\nUsed by the analytics dashboard.\n", p.View.Description)
}

func TestPipelines_LoadTests(t *testing.T) {
	yaml := `database:
  name: analytics
table:
  name: daily_revenue
tests:
  - name: daily_revenue_examples
    table: daily_revenue
    key: [day, country]
    expected:
      - { day: "2026-01-01", country: ES, revenue: 25, approved: true }
      - { day: "2026-01-02", country: ES, revenue: null, approved: false }
  - name: revenue_unique
    table: daily_revenue
    unique: [day, country]
    where: "country = 'ES'"
  - name: revenue_quality
    table: daily_revenue
    checks:
      - { field: revenue, not_null: true }
      - { field: revenue, between: [0, 100] }
      - { field: revenue, gt: 0 }
`
	path := filepath.Join(t.TempDir(), "tests.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	p := &pipelines.Pipelines{}
	require.NoError(t, p.LoadFile(path))

	require.Len(t, p.Tests, 3)
	assert.Equal(t, pipelines.Name("daily_revenue_examples"), p.Tests[0].Name)
	assert.Equal(t, []string{"day", "country"}, p.Tests[0].Key)
	assert.Equal(t, "ES", p.Tests[0].Expected[0]["country"])
	assert.Nil(t, p.Tests[0].Expected[1]["revenue"])
	assert.Equal(t, []string{"day", "country"}, p.Tests[1].Unique)
	assert.Equal(t, "country = 'ES'", p.Tests[1].Where.ToString())
	require.Len(t, p.Tests[2].Checks, 3)
	assert.True(t, p.Tests[2].Checks[0].NotNull)
	assert.Equal(t, []float64{0, 100}, p.Tests[2].Checks[1].Between)
	require.NotNil(t, p.Tests[2].Checks[2].GT)
	assert.Zero(t, *p.Tests[2].Checks[2].GT)
	for _, item := range p.Tests {
		assert.NoError(t, item.Validate())
	}
}
