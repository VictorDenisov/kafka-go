package kafka

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type TransactionManager struct {
	producerID      producerID
	coordinatorConn *Conn
	config          TransactionManagerConfig
	inTransaction   int32
}

type TransactionManagerConfig struct {
	TransactionalID string
	Brokers         []string
	Dialer          *Dialer
	ReadTimeout     time.Duration
}

func NewTransactionManager(config TransactionManagerConfig) *TransactionManager {
	if config.Dialer == nil {
		config.Dialer = DefaultDialer
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}
	return &TransactionManager{
		emptyProducerID,
		nil,
		config,
		0,
	}
}

func (t *TransactionManager) initTransactions() (err error) {
	var conn *Conn
	if conn, err = t.getCoordinatorConn(); err != nil {
		return
	}

	var producerIDResponse initProducerIDResponseV0
	if producerIDResponse, err = conn.initProducerID(t.config.TransactionalID); err != nil {
		return
	}
	if producerIDResponse.ErrorCode != 0 {
		return Error(producerIDResponse.ErrorCode)
	}
	t.producerID.ID = producerIDResponse.ProducerID
	t.producerID.Epoch = producerIDResponse.ProducerEpoch
	return nil
}

func (t *TransactionManager) getProducerID() producerID {
	if t == nil {
		return emptyProducerID
	}
	return t.producerID
}

func (t *TransactionManager) beginTransaction() (err error) {
	if len(t.config.TransactionalID) == 0 {
		return errors.New("Can't begin transaction in a non transactional writer.")
	}
	if !atomic.CompareAndSwapInt32(&t.inTransaction, 0, 1) {
		return errors.New("This writer already has a running transaction.")
	}

	return nil
}

func (t *TransactionManager) commitTransaction() (err error) {
	inTransaction := atomic.LoadInt32(&t.inTransaction)
	if inTransaction != 1 {
		return errors.New("The transaction is not started. Nothing to commit.")
	}
	var conn *Conn
	if conn, err = t.getCoordinatorConn(); err != nil {
		return
	}
	return conn.commitTransaction(t.config.TransactionalID, t.producerID)
}

func (t *TransactionManager) abortTransaction() (err error) {
	inTransaction := atomic.LoadInt32(&t.inTransaction)
	if inTransaction != 1 {
		return errors.New("The transaction is not started. Nothing to commit.")
	}
	var conn *Conn
	if conn, err = t.getCoordinatorConn(); err != nil {
		return
	}
	return conn.abortTransaction(t.config.TransactionalID, t.producerID)
}

func (t *TransactionManager) getCoordinatorConn() (conn *Conn, err error) {
	if t.coordinatorConn != nil {
		return t.coordinatorConn, nil
	}
	var coordinator findCoordinatorResponseCoordinatorV0
	if len(t.config.TransactionalID) != 0 {
		for _, broker := range shuffledStrings(t.config.Brokers) {
			if conn, err = t.config.Dialer.Dial("tcp", broker); err != nil {
				continue
			}

			conn.SetReadDeadline(time.Now().Add(t.config.ReadTimeout))
			coordinator, err = conn.findTransactionCoordinator(t.config.TransactionalID)
			conn.Close()

			if err == nil {
				break
			}
		}
	}

	if err != nil {
		return nil, err
	}
	addr := fmt.Sprintf("%v:%v", coordinator.Host, coordinator.Port)
	if conn, err = t.config.Dialer.Dial("tcp", addr); err != nil {
		// failed to connect to the coordinator
		return nil, err
	}

	t.coordinatorConn = conn
	return conn, nil
}
