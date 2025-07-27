package terminal

import (
	"bufio"
	"fmt"
	"os"
)

const (
	// Return cursor to start of line and clean it.
	LINE_RESET = "\r\033[K"
	// Erase the entire line.
	LINE_ERASE = "\x1B[2K"
	// Hide cursor.
	CURSOR_HIDE = "\x1b[?25l"
	// Show cursor.
	CURSOR_SHOW = "\x1b[?25h"
)

type Terminal struct {
	Output *bufio.Writer
}

func (t *Terminal) New() {
	t.Output = bufio.NewWriter(os.Stdout)
}

func (t *Terminal) Flush() {
	t.Output.Flush()
}

func (t *Terminal) LineReset() {
	t.Output.WriteString(LINE_RESET)
}

func (t *Terminal) LineErase() {
	t.Output.WriteString(LINE_ERASE)
	t.Flush()
}

func (t *Terminal) CursorHide() {
	t.Output.WriteString(CURSOR_HIDE)
	t.Flush()
}

func (t *Terminal) CursorShow() {
	t.Output.WriteString(CURSOR_SHOW)
	t.Flush()
}

func (t *Terminal) Write(format string, a ...any) {
	t.LineReset()
	t.Output.WriteString(fmt.Sprintf(format, a...))
	t.Flush()
}

func (t *Terminal) Rune(in rune) {
	t.Output.WriteRune(in)
	t.Flush()
}
