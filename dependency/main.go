package dependency

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/gather"
	"github.com/nicola-strappazzon/dacfy/pipelines"
)

var pl = pipelines.Instance()

func Views() error {
	tables := gather.Tables{}
	if err := tables.Load(pl.Database.Name.ToString()); err != nil {
		return err
	}

	for _, table := range tables {
		if table.Name == pl.View.Name.Suffix(pl.Config.Suffix).ToString() {
			continue
		}

		if table.To() == DatabaseTableName() {
			pl.Table.DependsOn = append(pl.Table.DependsOn, table.Name)
		}
	}

	return nil
}

func HasViews() error {
	if len(pl.Table.DependsOn) == 0 {
		return nil
	}

	return fmt.Errorf(
		"Cannot run swap command: the table %s is referenced by views: %v Please drop the views first before continuing.",
		pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString(),
		pl.Table.DependsOn,
	)
}

func DatabaseTableName() string {
	return fmt.Sprintf(
		"%s.%s",
		pl.Database.Name.ToString(),
		pl.Table.Name.Suffix(pl.Config.Suffix).ToString())
}
