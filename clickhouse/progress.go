package clickhouse

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nicola-strappazzon/dacfy/human"
)

type LoggerProgress interface {
	WriteProgress(in Progress)
}

type Progress struct {
	Start      time.Time
	ReadRows   uint64
	ReadBytes  uint64
	TotalRows  uint64
	Memory     uint64
	PeakMemory uint64
	CPU        float64
	Text       strings.Builder
}

var loggerProgress LoggerProgress = nil

func (p *Progress) StartNow() {
	p.Start = time.Now()
}

func (p *Progress) SetReadRows(in uint64) {
	p.ReadRows += in
}

func (p *Progress) SetReadBytes(in uint64) {
	p.ReadBytes += in
}

func (p *Progress) SetTotalRows(in uint64) {
	if in > 0 {
		p.TotalRows = in
	}
}

func (p Progress) Elapsed() time.Duration {
	return time.Since(p.Start)
}

func (p Progress) Percent() (out int) {
	if p.TotalRows == 0 {
		return 0
	}

	out = int(math.Round(float64(p.ReadRows) / float64(p.TotalRows) * 100))

	if out > 99 {
		return 100
	}

	return out
}

func (p Progress) ToString() string {
	p.Text = strings.Builder{}
	p.Text.Grow(100)
	p.Text.WriteString("\r\033[2K")

	if p.TotalRows > 0 {
		p.Text.WriteString("[")
		p.Text.WriteString(strconv.Itoa(p.Percent()))
		p.Text.WriteString("%%] ")
	}

	fmt.Fprintf(&p.Text, "%d ", p.ReadRows)

	if p.TotalRows > 0 {
		fmt.Fprintf(&p.Text, "of %d ", p.TotalRows)
	}

	p.Text.WriteString("Rows, ")
	p.Text.WriteString(human.Bytes(p.ReadBytes))
	p.Text.WriteString(", ")
	fmt.Fprintf(&p.Text, "%.2f CPU, ", p.CPU)
	p.Text.WriteString(human.Bytes(p.Memory))
	p.Text.WriteString(" RAM, Elapsed ")
	p.Text.WriteString(human.Duration(p.Elapsed()))

	return p.Text.String()
}
