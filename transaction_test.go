package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestEndTransactionResponseV0(t *testing.T) {
	item := endTransactionResponseV0{
		ThrottleTimeMs: 10,
		ErrorCode:      12,
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found endTransactionResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Fatal(err)
	}
	if remain != 0 {
		t.Fatalf("extpected 0 remain, got %v", remain)
	}
	if !reflect.DeepEqual(item, found) {
		t.Fatalf("expected item and found to be the same")
	}
}
