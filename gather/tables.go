package gather

import (
	"errors"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/strings"
)

var ch = clickhouse.Instance()

type Tables []Table

func (t Tables) GatherTablesStatement(database string) string {
	sql := strings.Builder{}
	sql.WriteString("SELECT ")
	sql.WriteString("database, name, engine, create_table_query, dependencies_database, dependencies_table ")
	sql.WriteString("FROM system.tables ")
	sql.WriteString("WHERE database = '")
	sql.WriteString(database)
	sql.WriteString("'")

	return sql.String()
}

func (t *Tables) Load(database string) error {
	var tbl = Table{}

	if ch.IsNotConnected() {
		return errors.New("Connection to ClickHouse server not established, you may be using the --dry-run flag.")
	}

	rows, err := ch.Connection.Query(ch.Context, t.GatherTablesStatement(database))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&tbl.Database,
			&tbl.Name,
			&tbl.Engine,
			&tbl.Create,
			&tbl.Dependencies.Databases,
			&tbl.Dependencies.Tables,
		)

		if err != nil {
			return err
		}
		t.Add(tbl)
	}

	return rows.Err()
}

func (t *Tables) Add(in Table) {
	*t = append(*t, in)
}

func (t Tables) Get(in string) Table {
	for index, table := range t {
		if table.Name == in {
			return t[index]
		}
	}

	return Table{}
}
