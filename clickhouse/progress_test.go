package clickhouse_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"

	"github.com/stretchr/testify/assert"
)

func TestStartNowSetsStart(t *testing.T) {
	var p clickhouse.Progress
	before := time.Now().Add(-1 * time.Second)
	p.StartNow()
	assert.WithinDuration(t, time.Now(), p.Start, time.Second)
	assert.True(t, p.Start.After(before))
}

func TestSetReadRowsAndBytes(t *testing.T) {
	var p clickhouse.Progress
	p.SetReadRows(10)
	p.SetReadRows(5)
	assert.EqualValues(t, 15, p.ReadRows)

	p.SetReadBytes(100)
	p.SetReadBytes(50)
	assert.EqualValues(t, 150, p.ReadBytes)
}

func TestSetTotalRowsOnlyWhenPositive(t *testing.T) {
	var p clickhouse.Progress
	p.SetTotalRows(0)
	assert.EqualValues(t, 0, p.TotalRows)

	p.SetTotalRows(200)
	assert.EqualValues(t, 200, p.TotalRows)

	p.SetTotalRows(0)
	assert.EqualValues(t, 200, p.TotalRows)
}

func TestElapsed(t *testing.T) {
	p := clickhouse.Progress{Start: time.Now().Add(-1500 * time.Millisecond)}
	el := p.Elapsed()
	assert.GreaterOrEqual(t, int64(el/time.Millisecond), int64(1400)) // ~1.5s
}

func TestPercentBasic(t *testing.T) {
	p := clickhouse.Progress{TotalRows: 0, ReadRows: 100}
	assert.Equal(t, 0, p.Percent(), "sin total => 0%")

	p = clickhouse.Progress{TotalRows: 200, ReadRows: 100}
	assert.Equal(t, 50, p.Percent(), "100/200 => 50%")

	p = clickhouse.Progress{TotalRows: 101, ReadRows: 50} // 49.5 => 50
	assert.Equal(t, 50, p.Percent())

	p = clickhouse.Progress{TotalRows: 100, ReadRows: 150} // 100
	assert.Equal(t, 100, p.Percent())
}

func TestToStringWithTotalRows(t *testing.T) {
	p := clickhouse.Progress{
		Start:     time.Now().Add(-2 * time.Second),
		ReadRows:  50,
		TotalRows: 100,
		ReadBytes: 1024 * 1024 * 1024, // 1 GiB
		Memory:    12 * 1024 * 1024,   // 12 MiB
		CPU:       23.375,
	}

	out := p.ToString()

	assert.True(t, strings.HasPrefix(out, "\r\033[2K"))
	assert.Contains(t, out, "[50%%] ")
	assert.Contains(t, out, "50 of 100")
	assert.Contains(t, out, "Rows, ")
	assert.Contains(t, out, "GiB")
	assert.Contains(t, out, "23.38 CPU")
	assert.Contains(t, out, "MiB RAM")
	assert.Contains(t, out, "Elapsed ")
}

func TestToStringWithoutTotalRows(t *testing.T) {
	p := clickhouse.Progress{
		Start:     time.Now().Add(-500 * time.Millisecond),
		ReadRows:  123,
		TotalRows: 0,
		ReadBytes: 10 * 1024,
		Memory:    1024 * 1024,
		CPU:       5.5,
	}

	out := p.ToString()

	assert.NotContains(t, out, "of ")
	assert.Contains(t, out, "123 ")
	assert.Contains(t, out, "5.50 CPU")
	assert.Contains(t, out, "RAM")
	assert.Contains(t, out, "Elapsed ")
}
