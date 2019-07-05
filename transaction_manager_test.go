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

func TestBeginTransaction(t *testing.T) {
	tm := newTransactionManager(transactionManagerConfig{
		transactionalID: "myTransaction",
		brokers:         []string{"localhost:9092"},
		dialer:          DefaultDialer,
		readTimeout:     10 * time.Second,
	})
	err := tm.beginTransaction()
	if err != nil {
		t.Fatalf("Dind't expect error in beginTransaction method: %v", err)
	}
	err = tm.beginTransaction()
	if err == nil {
		t.Fatalf("Beginning transaction twice should fail: %v", err)
	}
}

func TestCommitTransaction(t *testing.T) {
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
	err = tm.beginTransaction()
	if err != nil {
		t.Fatalf("Dind't expect error in beginTransaction method: %v", err)
	}
	err = tm.commitTransaction()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
