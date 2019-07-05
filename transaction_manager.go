package kafka

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type transactionManager struct {
	producerID    producerID
	conn          *Conn
	config        transactionManagerConfig
	inTransaction int32
}

type transactionManagerConfig struct {
	transactionalID string
	brokers         []string
	dialer          *Dialer
	readTimeout     time.Duration
}

func newTransactionManager(config transactionManagerConfig) *transactionManager {
	return &transactionManager{
		emptyProducerID,
		nil,
		config,
		0,
	}
}

func (t *transactionManager) initTransactions() (err error) {
	var conn *Conn
	if conn, err = t.getConnectionToCoordinator(); err != nil {
		return
	}

	var producerIDResponse initProducerIDResponseV0
	if producerIDResponse, err = conn.initProducerID(t.config.transactionalID); err != nil {
		return
	}
	if producerIDResponse.ErrorCode != 0 {
		return Error(producerIDResponse.ErrorCode)
	}
	t.producerID.ID = producerIDResponse.ProducerID
	t.producerID.Epoch = producerIDResponse.ProducerEpoch
	return nil
}

func (t *transactionManager) getProducerID() producerID {
	if t == nil {
		return emptyProducerID
	}
	return t.producerID
}

func (t *transactionManager) beginTransaction() (err error) {
	if len(t.config.transactionalID) == 0 {
		return errors.New("Can't begin transaction in a non transactional writer.")
	}
	if !atomic.CompareAndSwapInt32(&t.inTransaction, 0, 1) {
		return errors.New("This writer already has a running transaction.")
	}

	return nil
}

func (t *transactionManager) commitTransaction() (err error) {
	inTransaction := atomic.LoadInt32(&t.inTransaction)
	if inTransaction != 1 {
		return errors.New("The transaction is not started. Nothing to commit.")
	}
	var conn *Conn
	if conn, err = t.getConnectionToCoordinator(); err != nil {
		return
	}
	return conn.commitTransaction(t.config.transactionalID, t.producerID)
}

func (t *transactionManager) abortTransaction() (err error) {
	inTransaction := atomic.LoadInt32(&t.inTransaction)
	if inTransaction != 1 {
		return errors.New("The transaction is not started. Nothing to commit.")
	}
	var conn *Conn
	if conn, err = t.getConnectionToCoordinator(); err != nil {
		return
	}
	return conn.abortTransaction(t.config.transactionalID, t.producerID)
}

func (t *transactionManager) getConnectionToCoordinator() (conn *Conn, err error) {
	if t.conn != nil {
		return t.conn, nil
	}
	var coordinator findCoordinatorResponseCoordinatorV0
	if len(t.config.transactionalID) != 0 {
		for _, broker := range shuffledStrings(t.config.brokers) {
			if conn, err = t.config.dialer.Dial("tcp", broker); err != nil {
				continue
			}

			conn.SetReadDeadline(time.Now().Add(t.config.readTimeout))
			coordinator, err = conn.findTransactionCoordinator(t.config.transactionalID)
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
	if conn, err = t.config.dialer.Dial("tcp", addr); err != nil {
		// failed to connect to the coordinator
		return nil, err
	}

	t.conn = conn
	return conn, nil
}
