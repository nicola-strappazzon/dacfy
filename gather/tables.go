package gather

import (
	"errors"
	"regexp"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/strings"
)

var ch = clickhouse.Instance()
var re = regexp.MustCompile(`(?i)TO\s+([a-zA-Z0-9_\.]+)`)

type Table struct {
	Database string
	Name     string
	Engine   string
	Create   string
}

type Tables []Table

func (t Table) To() string {
	if match := re.FindStringSubmatch(t.Create); len(match) > 1 {
		return match[1]
	}

	return ""
}

func (t Table) Statement() string {
	sql := strings.Builder{}
	sql.WriteString("SELECT name, engine, create_table_query ")
	sql.WriteString("FROM system.tables ")
	sql.WriteString("WHERE database = '")
	sql.WriteString(t.Database)
	sql.WriteString("'")

	return sql.String()
}

func (t *Tables) Add(in Table) {
	*t = append(*t, in)
}

func (t *Tables) Load(database string) error {
	if ch.IsNotConnected() {
		return errors.New("Connection to ClickHouse server not established, you may be using the --dry-run flag.")
	}

	tbl := Table{}
	tbl.Database = database

	rows, err := ch.Connection.Query(ch.Context, tbl.Statement())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&tbl.Name, &tbl.Engine, &tbl.Create); err != nil {
			return err
		}
		t.Add(tbl)
	}

	return rows.Err()
}
