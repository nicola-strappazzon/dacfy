package progress

import (
	"sync"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/terminal"
)

type writer interface {
	WriteProgress(clickhouse.Progress)
}

type terminalWriter struct {
	terminal terminal.Terminal
}

func (w *terminalWriter) WriteProgress(in clickhouse.Progress) {
	w.terminal.New()
	w.terminal.Write(in.ToString())
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
