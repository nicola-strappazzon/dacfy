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
