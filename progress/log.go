package progress

import (
	"fmt"
	"io"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/human"
)

// logWriter keeps the latest metrics and writes one line after a successful run.
type logWriter struct {
	out      io.Writer
	progress clickhouse.Progress
}

func NewLogHandler(out io.Writer) *Handler {
	return newHandler(&logWriter{out: out})
}

func (w *logWriter) WriteProgress(in clickhouse.Progress) {
	w.progress = in
}

func (w *logWriter) FinishProgress() {
	w.progress = clickhouse.Progress{}
}

func (w *logWriter) CompleteProgress() {
	p := w.progress
	fmt.Fprintf(w.out, "%s %d Rows, %s, %.2f CPU, %s RAM, Elapsed %s\n",
		time.Now().Format(time.RFC3339), p.ReadRows, human.Bytes(p.ReadBytes),
		p.CPU, human.Bytes(p.Memory), human.Duration(p.Elapsed()))
	w.progress = clickhouse.Progress{}
}
