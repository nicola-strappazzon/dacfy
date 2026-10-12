package progress

import (
	"sync"
	"testing"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"

	"github.com/stretchr/testify/assert"
)

type concurrentWriter struct {
	mu        sync.Mutex
	active    int
	maxActive int
	writes    int
}

func (w *concurrentWriter) WriteProgress(clickhouse.Progress) {
	w.mu.Lock()
	w.active++
	w.writes++
	if w.active > w.maxActive {
		w.maxActive = w.active
	}
	w.mu.Unlock()

	time.Sleep(time.Millisecond)

	w.mu.Lock()
	w.active--
	w.mu.Unlock()
}

func TestHandlerSerializesProgressWrites(t *testing.T) {
	writer := &concurrentWriter{}
	handler := newHandler(writer)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.WriteProgress(clickhouse.Progress{})
		}()
	}
	wg.Wait()

	assert.Equal(t, 10, writer.writes)
	assert.Equal(t, 1, writer.maxActive)
}
