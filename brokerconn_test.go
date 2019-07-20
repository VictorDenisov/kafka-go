package kafka

import (
	"log"
	"net"
	"testing"

	"context"
)

func TestApiVersions(t *testing.T) {
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", "localhost:9092")
	if err != nil {
		t.Fatalf("Failed to connect to broker: %v", err)
	}
	bc := NewBrokerConn(conn, BrokerConnConfig{})
	versions, err := bc.ApiVersions()
	if err != nil {
		t.Fatalf("Failed receive api versions: %v", err)
	}

	log.Printf("versions: %v", versions)
}
