package kafka

import (
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

	if apiVersion(versions[fetchRequest].MinVersion) > bc.fetchVersion ||
		apiVersion(versions[fetchRequest].MaxVersion) < bc.fetchVersion {
		t.Fatalf("fetchVersion is outside of apiVersions interval")
	}
}
