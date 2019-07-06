package kafka

import (
	"testing"
	"time"
)

func TestInitTransactions(t *testing.T) {
	tm := NewTransactionManager(TransactionManagerConfig{
		TransactionalID: "myTransaction",
		Brokers:         []string{"localhost:9092"},
		Dialer:          DefaultDialer,
		ReadTimeout:     10 * time.Second,
	})
	err := tm.initTransactions()

	if err != nil {
		t.Fatalf("Didn't expect error in initTransactions method: %v", err)
	}
}

func TestBeginTransaction(t *testing.T) {
	tm := NewTransactionManager(TransactionManagerConfig{
		TransactionalID: "myTransaction",
		Brokers:         []string{"localhost:9092"},
		Dialer:          DefaultDialer,
		ReadTimeout:     10 * time.Second,
	})
	err := tm.beginTransaction()
	if err != nil {
		t.Fatalf("Didn't expect error in beginTransaction method: %v", err)
	}
	err = tm.beginTransaction()
	if err == nil {
		t.Fatalf("Beginning transaction twice should fail: %v", err)
	}
}

func TestCommitTransaction(t *testing.T) {
	tm := NewTransactionManager(TransactionManagerConfig{
		TransactionalID: "myTransaction",
		Brokers:         []string{"localhost:9092"},
		Dialer:          DefaultDialer,
		ReadTimeout:     10 * time.Second,
	})
	err := tm.initTransactions()

	if err != nil {
		t.Fatalf("Didn't expect error in initTransactions method: %v", err)
	}
	err = tm.beginTransaction()
	if err != nil {
		t.Fatalf("Didn't expect error in beginTransaction method: %v", err)
	}
	err = tm.commitTransaction()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestAbortTransaction(t *testing.T) {
	tm := NewTransactionManager(TransactionManagerConfig{
		TransactionalID: "myTransaction",
		Brokers:         []string{"localhost:9092"},
		Dialer:          DefaultDialer,
		ReadTimeout:     10 * time.Second,
	})
	err := tm.initTransactions()

	if err != nil {
		t.Fatalf("Didn't expect error in initTransactions method: %v", err)
	}
	err = tm.beginTransaction()
	if err != nil {
		t.Fatalf("Didn't expect error in beginTransaction method: %v", err)
	}
	err = tm.abortTransaction()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
