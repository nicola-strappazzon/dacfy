package terminal

import (
	"bufio"
	"fmt"
	"os"
)

const (
	LINE_ERASE = "\x1B[2K"
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

func (t *Terminal) LineErase() {
	t.Output.WriteString(LINE_ERASE)
	t.Flush()
}

func (t *Terminal) Write(format string, a ...any) {
	t.LineErase()
	t.Output.WriteString(fmt.Sprintf(format, a...))
	t.Flush()
}

func (t *Terminal) Rune(in rune) {
	t.Output.WriteRune(in)
	t.Flush()
}
