package kafka

import (
	"bufio"
	"testing"
)

func TestMessageSetReaderEmpty(t *testing.T) {
	m := messageSetReader{empty: true}

	noop := func(*bufio.Reader, int, int) (int, error) {
		return 0, nil
	}

	meta, err := m.readMessage(0, noop, noop)
	if meta.Offset != 0 {
		t.Errorf("expected offset of 0, get %d", meta.Offset)
	}
	if meta.Timestamp != 0 {
		t.Errorf("expected timestamp of 0, get %d", meta.Timestamp)
	}
	if meta.Headers != nil {
		t.Errorf("expected nil headers, got %v", meta.Headers)
	}
	if err != errShortRead {
		t.Errorf("expected errShortRead, got %v", err)
	}

	if m.remaining() != 0 {
		t.Errorf("expected 0 remaining, got %d", m.remaining())
	}

	if m.discard() != nil {
		t.Errorf("unexpected error from discard(): %v", m.discard())
	}
}
