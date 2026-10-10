package terminal

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteTruncatesProgressToTerminalWidth(t *testing.T) {
	var output bytes.Buffer
	terminal := Terminal{
		Output: bufio.NewWriter(&output),
		Width:  80,
	}

	terminal.Write("\r" + LINE_ERASE + strings.Repeat("x", 100))

	assert.Equal(t, "\r"+LINE_ERASE+strings.Repeat("x", 77)+"...", output.String())
}

func TestTruncateLeavesTextUnchangedWhenWidthIsUnknown(t *testing.T) {
	assert.Equal(t, "progress", truncate("progress", 0))
}
