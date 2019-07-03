package kafka

import (
	"testing"
	"time"
)

func TestInitTransactions(t *testing.T) {
	tm := newTransactionManager(transactionManagerConfig{
		transactionalID: "myTransaction",
		brokers:         []string{"localhost:9092"},
		dialer:          DefaultDialer,
		readTimeout:     10 * time.Second,
	})
	err := tm.initTransactions()

	if err != nil {
		t.Fatalf("Dind't expect error in initTransactions method: %v", err)
	}
}
