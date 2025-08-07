package clickhouse

import (
	"time"
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

func (p Progress) Percent() float64 {
	return float64(p.ReadRows) / float64(p.TotalRows) * 100
}
