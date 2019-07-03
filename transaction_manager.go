package kafka

import (
	"fmt"
	"time"
)

type transactionManager struct {
	producerID producerID
	conn       *Conn
	config     transactionManagerConfig
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
	}
}

func (t *transactionManager) initTransactions() (err error) {
	if t.conn, err = t.getConnectionToCoordinator(); err != nil {
		return
	}

	var producerIDResponse initProducerIDResponseV0
	if producerIDResponse, err = t.conn.initProducerID(t.config.transactionalID); err != nil {
		return
	}
	if producerIDResponse.ErrorCode != 0 {
		return Error(producerIDResponse.ErrorCode)
	}
	t.producerID.ID = producerIDResponse.ProducerID
	t.producerID.Epoch = producerIDResponse.ProducerEpoch
	return nil
}

func (t *transactionManager) getConnectionToCoordinator() (conn *Conn, err error) {
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

	return conn, nil
}
