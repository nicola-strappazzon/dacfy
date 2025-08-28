package rebuild

import (
	"fmt"

	// "github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/spf13/cobra"
)

var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "rebuild",
		Short:   "Rebuild table data by partition to recalculate materialized views.",
		Example: `dacfy rebuild --partition 20250730 foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Table.PartitionID, "partition", "", "Partition id to rebuild (e.g. 20250730)")

	return cmd
}

func Run() (err error) {
	if err = pl.Database.Validate(); err != nil {
		return err
	}

	if err = pl.Table.Validate(); err != nil {
		return err
	}

	var dummy = pipelines.Table{
		Name:        pipelines.Name(pl.Table.Name.ToString() + "_dummy"),
		PartitionID: pl.Table.PartitionID,
	}

	if len(pl.Table.PartitionID) == 0 {

	} else {

	}

	queries := []struct {
		// Name      string
		// Message   string
		Statement string
		// Rows      int
	}{
		{
			Statement: dummy.CopyFrom(pl.Table).DML(),
		},
		{
			Statement: pl.Table.RowsOnPartition().DML(),
		},
		{
			Statement: dummy.AttachPartitionTo(pl.Table).DML(),
		},
		{
			Statement: pl.Table.DetachPartition().DML(),
		},
		{
			Statement: pl.Table.RowsOnPartition().DML(),
		},
		{
			Statement: dummy.RowsOnPartition().DML(),
		},
		{
			Statement: pl.Table.InsertIntoSelectFrom(dummy).DML(),
		},
		{
			Statement: pl.Table.RowsOnPartition().DML(),
		},
		{
			Statement: dummy.DetachPartition().DML(),
		},
	}

	for _, query := range queries {
		fmt.Println(query.Statement)
	}

	return err
}
