package progress

import (
	"sync"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/terminal"
)

type writer interface {
	CompleteProgress()
	FinishProgress()
	WriteProgress(clickhouse.Progress)
}

type terminalWriter struct {
	terminal terminal.Terminal
	wrote    bool
}

func (w *terminalWriter) WriteProgress(in clickhouse.Progress) {
	w.terminal.New()
	w.terminal.Write(in.ToString())
	w.wrote = true
}

func (w *terminalWriter) FinishProgress() {
	if !w.wrote {
		return
	}

	w.terminal.Output.WriteString("\n")
	w.terminal.Flush()
	w.wrote = false
}

func (w *terminalWriter) CompleteProgress() {
	w.wrote = false
}

type Handler struct {
	mu     sync.Mutex
	writer writer
}

func NewHandler() *Handler {
	return newHandler(&terminalWriter{})
}

func newHandler(writer writer) *Handler {
	return &Handler{writer: writer}
}

func (h *Handler) WriteProgress(in clickhouse.Progress) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.writer.WriteProgress(in)
}

func (h *Handler) FinishProgress() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.writer.FinishProgress()
}

func (h *Handler) CompleteProgress() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.writer.CompleteProgress()
}
