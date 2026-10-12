package terminal

import (
	"bufio"
	"os"
	"strings"

	"golang.org/x/term"
)

const (
	LINE_ERASE = "\x1B[2K"
)

type Terminal struct {
	Output *bufio.Writer
	Width  int
}

func (t *Terminal) New() {
	t.Output = bufio.NewWriter(os.Stdout)
	t.Width, _, _ = term.GetSize(int(os.Stdout.Fd()))
}

func (t *Terminal) Flush() {
	t.Output.Flush()
}

func (t *Terminal) LineErase() {
	t.Output.WriteString(LINE_ERASE)
	t.Flush()
}

func (t *Terminal) Write(in string) {
	in = strings.TrimPrefix(in, "\r"+LINE_ERASE)
	in = truncate(in, t.Width)
	t.Output.WriteString("\r")
	t.LineErase()
	t.Output.WriteString(in)
	t.Flush()
}

func truncate(in string, width int) string {
	if width <= 0 || len(in) <= width {
		return in
	}

	if width <= 3 {
		return in[:width]
	}

	return in[:width-3] + "..."
}
