package kafka

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errInvalidWriteTopic     = errors.New("writes must NOT set Topic on kafka.Message")
	errInvalidWritePartition = errors.New("writes must NOT set Partition on kafka.Message")
)

// Broker carries the metadata associated with a kafka broker.
type Broker struct {
	Host string
	Port int
	ID   int
	Rack string
}

// Partition carries the metadata associated with a kafka partition.
type Partition struct {
	Topic    string
	Leader   Broker
	Replicas []Broker
	Isr      []Broker
	ID       int
}

// Conn represents a connection to a kafka broker.
//
// Instances of Conn are safe to use concurrently from multiple goroutines.
type Conn struct {
	brokerConn *BrokerConn

	// offset management (synchronized on the mutex field)
	mutex  sync.Mutex
	offset int64

	// immutable values of the connection object
	topic         string
	partition     int32
	fetchMaxBytes int32
	fetchMinSize  int32

	// number of replica acks required when publishing to a partition
	requiredAcks int32

	transactionalID *string
}

// ConnConfig is a configuration object used to create new instances of Conn.
type ConnConfig struct {
	ClientID  string
	Topic     string
	Partition int

	// The transactional id to use for transactional delivery. Idempotent
	// deliver should be enabled if transactional id is configured.
	// For more details look at transactional.id description here: http://kafka.apache.org/documentation.html#producerconfigs
	// Empty string means that this connection can't be transactional.
	TransactionalID string
}

// ReadBatchConfig is a configuration object used for reading batches of messages.
type ReadBatchConfig struct {
	MinBytes int
	MaxBytes int

	// IsolationLevel controls the visibility of transactional records.
	// ReadUncommitted makes all records visible. With ReadCommitted only
	// non-transactional and committed records are visible.
	IsolationLevel IsolationLevel
}

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = 0
	ReadCommitted   IsolationLevel = 1
)

var (
	// DefaultClientID is the default value used as ClientID of kafka
	// connections.
	DefaultClientID string
)

func init() {
	progname := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()
	DefaultClientID = fmt.Sprintf("%s@%s (github.com/segmentio/kafka-go)", progname, hostname)
}

// NewConn returns a new kafka connection for the given topic and partition.
func NewConn(conn net.Conn, topic string, partition int) *Conn {
	return NewConnWith(conn, ConnConfig{
		Topic:     topic,
		Partition: partition,
	})
}

func emptyToNullable(transactionalID string) (result *string) {
	if transactionalID != "" {
		result = &transactionalID
	}
	return result
}

// NewConnWith returns a new kafka connection configured with config.
// The offset is initialized to FirstOffset.
func NewConnWith(conn net.Conn, config ConnConfig) *Conn {
	if len(config.ClientID) == 0 {
		config.ClientID = DefaultClientID
	}

	if config.Partition < 0 || config.Partition > math.MaxInt32 {
		panic(fmt.Sprintf("invalid partition number: %d", config.Partition))
	}

	c := &Conn{
		brokerConn: NewBrokerConn(conn, BrokerConnConfig{
			ClientID: config.ClientID,
		}),
		topic:           config.Topic,
		partition:       int32(config.Partition),
		offset:          FirstOffset,
		requiredAcks:    -1,
		transactionalID: emptyToNullable(config.TransactionalID),
	}

	// The fetch request needs to ask for a MaxBytes value that is at least
	// enough to load the control data of the response. To avoid having to
	// recompute it on every read, it is cached here in the Conn value.
	c.fetchMinSize = (fetchResponseV2{
		Topics: []fetchResponseTopicV2{{
			TopicName: config.Topic,
			Partitions: []fetchResponsePartitionV2{{
				Partition:  int32(config.Partition),
				MessageSet: messageSet{{}},
			}},
		}},
	}).size()
	c.fetchMaxBytes = math.MaxInt32 - c.fetchMinSize
	return c
}

func (c *Conn) Controller() (broker Broker, err error) {
	return c.brokerConn.Controller()
}

// Brokers retrieve the broker list from the Kafka metadata
func (c *Conn) Brokers() ([]Broker, error) {
	return c.brokerConn.Brokers()
}

// DeleteTopics deletes the specified topics.
func (c *Conn) DeleteTopics(topics ...string) error {
	_, err := c.deleteTopics(deleteTopicsRequestV0{
		Topics: topics,
	})
	return err
}

// describeGroups retrieves the specified groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
func (c *Conn) describeGroups(request describeGroupsRequestV0) (describeGroupsResponseV0, error) {
	return c.brokerConn.describeGroups(request)
}

// findCoordinator finds the coordinator for the specified group or transaction
//
// See http://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
func (c *Conn) findCoordinator(request findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
	return c.brokerConn.findCoordinator(request)
}

// heartbeat sends a heartbeat message required by consumer groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_Heartbeat
func (c *Conn) heartbeat(request heartbeatRequestV0) (heartbeatResponseV0, error) {
	return c.brokerConn.heartbeat(request)
}

// joinGroup attempts to join a consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_JoinGroup
func (c *Conn) joinGroup(request joinGroupRequestV1) (joinGroupResponseV1, error) {
	return c.brokerConn.joinGroup(request)
}

// leaveGroup leaves the consumer from the consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_LeaveGroup
func (c *Conn) leaveGroup(request leaveGroupRequestV0) (leaveGroupResponseV0, error) {
	return c.brokerConn.leaveGroup(request)
}

// listGroups lists all the consumer groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_ListGroups
func (c *Conn) listGroups(request listGroupsRequestV1) (listGroupsResponseV1, error) {
	return c.brokerConn.listGroups(request)
}

// offsetCommit commits the specified topic partition offsets
//
// See http://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
func (c *Conn) offsetCommit(request offsetCommitRequestV2) (offsetCommitResponseV2, error) {
	return c.brokerConn.offsetCommit(request)
}

// offsetFetch fetches the offsets for the specified topic partitions.
// -1 indicates that there is no offset saved for the partition.
//
// See http://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
func (c *Conn) offsetFetch(request offsetFetchRequestV1) (offsetFetchResponseV1, error) {
	return c.brokerConn.offsetFetch(request)
}

// syncGroups completes the handshake to join a consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_SyncGroup
func (c *Conn) syncGroups(request syncGroupRequestV0) (syncGroupResponseV0, error) {
	return c.brokerConn.syncGroups(request)
}

// Close closes the kafka connection.
func (c *Conn) Close() error {
	return c.brokerConn.Close()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.brokerConn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.brokerConn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated with the connection.
// It is equivalent to calling both SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations fail with a timeout
// (see type Error) instead of blocking. The deadline applies to all future and
// pending I/O, not just the immediately following call to Read or Write. After
// a deadline has been exceeded, the connection may be closed if it was found to
// be in an unrecoverable state.
//
// A zero value for t means I/O operations will not time out.
func (c *Conn) SetDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	c.wdeadline.setDeadline(t)
	return nil
}

// SetReadDeadline sets the deadline for future Read calls and any
// currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls and any
// currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that some of the
// data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wdeadline.setDeadline(t)
	return nil
}

// Offset returns the current offset of the connection as pair of integers,
// where the first one is an offset value and the second one indicates how
// to interpret it.
//
// See Seek for more details about the offset and whence values.
func (c *Conn) Offset() (offset int64, whence int) {
	c.mutex.Lock()
	offset = c.offset
	c.mutex.Unlock()

	switch offset {
	case FirstOffset:
		offset = 0
		whence = SeekStart
	case LastOffset:
		offset = 0
		whence = SeekEnd
	default:
		whence = SeekAbsolute
	}
	return
}

const (
	SeekStart    = 0 // Seek relative to the first offset available in the partition.
	SeekAbsolute = 1 // Seek to an absolute offset.
	SeekEnd      = 2 // Seek relative to the last offset available in the partition.
	SeekCurrent  = 3 // Seek relative to the current offset.

	// This flag may be combined to any of the SeekAbsolute and SeekCurrent
	// constants to skip the bound check that the connection would do otherwise.
	// Programs can use this flag to avoid making a metadata request to the kafka
	// broker to read the current first and last offsets of the partition.
	SeekDontCheck = 1 << 31
)

// Seek sets the offset for the next read or write operation according to whence, which
// should be one of SeekStart, SeekAbsolute, SeekEnd, or SeekCurrent.
// When seeking relative to the end, the offset is subtracted from the current offset.
// Note that for historical reasons, these do not align with the usual whence constants
// as in lseek(2) or os.Seek.
// The method returns the new absolute offset of the connection.
func (c *Conn) Seek(offset int64, whence int) (int64, error) {
	seekDontCheck := (whence & SeekDontCheck) != 0
	whence &= ^SeekDontCheck

	switch whence {
	case SeekStart, SeekAbsolute, SeekEnd, SeekCurrent:
	default:
		return 0, fmt.Errorf("whence must be one of 0, 1, 2, or 3. (whence = %d)", whence)
	}

	if seekDontCheck {
		if whence == SeekAbsolute {
			c.mutex.Lock()
			c.offset = offset
			c.mutex.Unlock()
			return offset, nil
		}

		if whence == SeekCurrent {
			c.mutex.Lock()
			c.offset += offset
			offset = c.offset
			c.mutex.Unlock()
			return offset, nil
		}
	}

	if whence == SeekAbsolute {
		c.mutex.Lock()
		unchanged := offset == c.offset
		c.mutex.Unlock()
		if unchanged {
			return offset, nil
		}
	}

	if whence == SeekCurrent {
		c.mutex.Lock()
		offset = c.offset + offset
		c.mutex.Unlock()
	}

	first, last, err := c.ReadOffsets()
	if err != nil {
		return 0, err
	}

	switch whence {
	case SeekStart:
		offset = first + offset
	case SeekEnd:
		offset = last - offset
	}

	if offset < first || offset > last {
		return 0, OffsetOutOfRange
	}

	c.mutex.Lock()
	c.offset = offset
	c.mutex.Unlock()
	return offset, nil
}

// Read reads the message at the current offset from the connection, advancing
// the offset on success so the next call to a read method will produce the next
// message.
// The method returns the number of bytes read, or an error if something went
// wrong.
//
// While it is safe to call Read concurrently from multiple goroutines it may
// be hard for the program to predict the results as the connection offset will
// be read and written by multiple goroutines, they could read duplicates, or
// messages may be seen by only some of the goroutines.
//
// The method fails with io.ErrShortBuffer if the buffer passed as argument is
// too small to hold the message value.
//
// This method is provided to satisfy the net.Conn interface but is much less
// efficient than using the more general purpose ReadBatch method.
func (c *Conn) Read(b []byte) (int, error) {
	batch := c.ReadBatch(1, len(b))
	n, err := batch.Read(b)
	return n, coalesceErrors(silentEOF(err), batch.Close())
}

// ReadMessage reads the message at the current offset from the connection,
// advancing the offset on success so the next call to a read method will
// produce the next message.
//
// Because this method allocate memory buffers for the message key and value
// it is less memory-efficient than Read, but has the advantage of never
// failing with io.ErrShortBuffer.
//
// While it is safe to call Read concurrently from multiple goroutines it may
// be hard for the program to predict the results as the connection offset will
// be read and written by multiple goroutines, they could read duplicates, or
// messages may be seen by only some of the goroutines.
//
// This method is provided for convenience purposes but is much less efficient
// than using the more general purpose ReadBatch method.
func (c *Conn) ReadMessage(maxBytes int) (Message, error) {
	batch := c.ReadBatch(1, maxBytes)
	msg, err := batch.ReadMessage()
	return msg, coalesceErrors(silentEOF(err), batch.Close())
}

// ReadBatch reads a batch of messages from the kafka server. The method always
// returns a non-nil Batch value. If an error occurred, either sending the fetch
// request or reading the response, the error will be made available by the
// returned value of  the batch's Close method.
//
// While it is safe to call ReadBatch concurrently from multiple goroutines it
// may be hard for the program to predict the results as the connection offset
// will be read and written by multiple goroutines, they could read duplicates,
// or messages may be seen by only some of the goroutines.
//
// A program doesn't specify the number of messages in wants from a batch, but
// gives the minimum and maximum number of bytes that it wants to receive from
// the kafka server.
func (c *Conn) ReadBatch(minBytes, maxBytes int) *Batch {
	return c.ReadBatchWith(ReadBatchConfig{
		MinBytes: minBytes,
		MaxBytes: maxBytes,
	})
}

// ReadBatchWith in every way is similar to ReadBatch. ReadBatch is configured
// with the default values in ReadBatchConfig except for minBytes and maxBytes.
func (c *Conn) ReadBatchWith(cfg ReadBatchConfig) *Batch {

	var adjustedDeadline time.Time
	var maxFetch = int(c.fetchMaxBytes)

	if cfg.MinBytes < 0 || cfg.MinBytes > maxFetch {
		return &Batch{err: fmt.Errorf("kafka.(*Conn).ReadBatch: minBytes of %d out of [1,%d] bounds", cfg.MinBytes, maxFetch)}
	}
	if cfg.MaxBytes < 0 || cfg.MaxBytes > maxFetch {
		return &Batch{err: fmt.Errorf("kafka.(*Conn).ReadBatch: maxBytes of %d out of [1,%d] bounds", cfg.MaxBytes, maxFetch)}
	}
	if cfg.MinBytes > cfg.MaxBytes {
		return &Batch{err: fmt.Errorf("kafka.(*Conn).ReadBatch: minBytes (%d) > maxBytes (%d)", cfg.MinBytes, cfg.MaxBytes)}
	}

	offset, whence := c.Offset()

	offset, err := c.Seek(offset, whence|SeekDontCheck)
	if err != nil {
		return &Batch{err: dontExpectEOF(err)}
	}

	id, err := c.doRequest(&c.rdeadline, func(deadline time.Time, id int32) error {
		now := time.Now()
		deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
		adjustedDeadline = deadline
		switch c.fetchVersion {
		case v10:
			return writeFetchRequestV10(
				&c.wbuf,
				id,
				c.clientID,
				c.topic,
				c.partition,
				offset,
				cfg.MinBytes,
				cfg.MaxBytes+int(c.fetchMinSize),
				deadlineToTimeout(deadline, now),
				int8(cfg.IsolationLevel),
			)
		case v5:
			return writeFetchRequestV5(
				&c.wbuf,
				id,
				c.clientID,
				c.topic,
				c.partition,
				offset,
				cfg.MinBytes,
				cfg.MaxBytes+int(c.fetchMinSize),
				deadlineToTimeout(deadline, now),
				int8(cfg.IsolationLevel),
			)
		default:
			return writeFetchRequestV2(
				&c.wbuf,
				id,
				c.clientID,
				c.topic,
				c.partition,
				offset,
				cfg.MinBytes,
				cfg.MaxBytes+int(c.fetchMinSize),
				deadlineToTimeout(deadline, now),
			)
		}
	})
	if err != nil {
		return &Batch{err: dontExpectEOF(err)}
	}

	_, size, lock, err := c.waitResponse(&c.rdeadline, id)
	if err != nil {
		return &Batch{err: dontExpectEOF(err)}
	}

	var throttle int32
	var highWaterMark int64
	var remain int

	switch c.fetchVersion {
	case v10:
		throttle, highWaterMark, remain, err = readFetchResponseHeaderV10(&c.rbuf, size)
	case v5:
		throttle, highWaterMark, remain, err = readFetchResponseHeaderV5(&c.rbuf, size)
	default:
		throttle, highWaterMark, remain, err = readFetchResponseHeaderV2(&c.rbuf, size)
	}
	if err == errShortRead {
		err = checkTimeoutErr(adjustedDeadline)
	}

	var msgs *messageSetReader
	if err == nil {
		if highWaterMark == offset {
			msgs = &messageSetReader{empty: true}
		} else {
			msgs, err = newMessageSetReader(&c.rbuf, remain)
		}
	}
	if err == errShortRead {
		err = checkTimeoutErr(adjustedDeadline)
	}
	return &Batch{
		conn:          c,
		msgs:          msgs,
		deadline:      adjustedDeadline,
		throttle:      duration(throttle),
		lock:          lock,
		topic:         c.topic,          // topic is copied to Batch to prevent race with Batch.close
		partition:     int(c.partition), // partition is copied to Batch to prevent race with Batch.close
		offset:        offset,
		highWaterMark: highWaterMark,
		err:           dontExpectEOF(err),
	}
}

// ReadOffset returns the offset of the first message with a timestamp equal or
// greater to t.
func (c *Conn) ReadOffset(t time.Time) (int64, error) {
	return c.readOffset(timestamp(t))
}

// ReadFirstOffset returns the first offset available on the connection.
func (c *Conn) ReadFirstOffset() (int64, error) {
	return c.readOffset(FirstOffset)
}

// ReadLastOffset returns the last offset available on the connection.
func (c *Conn) ReadLastOffset() (int64, error) {
	return c.readOffset(LastOffset)
}

// ReadOffsets returns the absolute first and last offsets of the topic used by
// the connection.
func (c *Conn) ReadOffsets() (first, last int64, err error) {
	// We have to submit two different requests to fetch the first and last
	// offsets because kafka refuses requests that ask for multiple offsets
	// on the same topic and partition.
	if first, err = c.ReadFirstOffset(); err != nil {
		return
	}
	if last, err = c.ReadLastOffset(); err != nil {
		first = 0 // don't leak the value on error
		return
	}
	return
}

func (c *Conn) readOffset(t int64) (offset int64, err error) {
	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			return writeListOffsetRequestV1(&c.wbuf, id, c.clientID, c.topic, c.partition, t)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(readArrayWith(&c.rbuf, size, func(r *bufio.Reader, size int) (int, error) {
				// We skip the topic name because we've made a request for
				// a single topic.
				size, err := discardString(r, size)
				if err != nil {
					return size, err
				}

				// Reading the array of partitions, there will be only one
				// partition which gives the offset we're looking for.
				return readArrayWith(r, size, func(r *bufio.Reader, size int) (int, error) {
					var p partitionOffsetV1
					size, err := p.readFrom(r, size)
					if err != nil {
						return size, err
					}
					if p.ErrorCode != 0 {
						return size, Error(p.ErrorCode)
					}
					offset = p.Offset
					return size, nil
				})
			}))
		},
	)
	return
}

// ReadPartitions returns the list of available partitions for the given list of
// topics.
//
// If the method is called with no topic, it uses the topic configured on the
// connection. If there are none, the method fetches all partitions of the kafka
// cluster.
func (c *Conn) ReadPartitions(topics ...string) (partitions []Partition, err error) {
	defaultTopics := [...]string{c.topic}

	if len(topics) == 0 && len(c.topic) != 0 {
		topics = defaultTopics[:]
	}

	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(metadataRequest, v1, id, topicMetadataRequestV1(topics))
		},
		func(deadline time.Time, size int) error {
			var res metadataResponseV1

			if err := c.readResponse(size, &res); err != nil {
				return err
			}

			brokers := make(map[int32]Broker, len(res.Brokers))
			for _, b := range res.Brokers {
				brokers[b.NodeID] = Broker{
					Host: b.Host,
					Port: int(b.Port),
					ID:   int(b.NodeID),
					Rack: b.Rack,
				}
			}

			makeBrokers := func(ids ...int32) []Broker {
				b := make([]Broker, len(ids))
				for i, id := range ids {
					b[i] = brokers[id]
				}
				return b
			}

			for _, t := range res.Topics {
				if t.TopicErrorCode != 0 && (c.topic == "" || t.TopicName == c.topic) {
					// We only report errors if they happened for the topic of
					// the connection, otherwise the topic will simply have no
					// partitions in the result set.
					return Error(t.TopicErrorCode)
				}
				for _, p := range t.Partitions {
					partitions = append(partitions, Partition{
						Topic:    t.TopicName,
						Leader:   brokers[p.Leader],
						Replicas: makeBrokers(p.Replicas...),
						Isr:      makeBrokers(p.Isr...),
						ID:       int(p.PartitionID),
					})
				}
			}
			return nil
		},
	)
	return
}

// Write writes a message to the kafka broker that this connection was
// established to. The method returns the number of bytes written, or an error
// if something went wrong.
//
// The operation either succeeds or fail, it never partially writes the message.
//
// This method is exposed to satisfy the net.Conn interface but is less efficient
// than the more general purpose WriteMessages method.
func (c *Conn) Write(b []byte) (int, error) {
	return c.WriteCompressedMessages(nil, Message{Value: b})
}

// WriteMessages writes a batch of messages to the connection's topic and
// partition, returning the number of bytes written. The write is an atomic
// operation, it either fully succeeds or fails.
func (c *Conn) WriteMessages(msgs ...Message) (int, error) {
	return c.WriteCompressedMessages(nil, msgs...)
}

// WriteCompressedMessages writes a batch of messages to the connection's topic
// and partition, returning the number of bytes written. The write is an atomic
// operation, it either fully succeeds or fails.
//
// If the compression codec is not nil, the messages will be compressed.
func (c *Conn) WriteCompressedMessages(codec CompressionCodec, msgs ...Message) (nbytes int, err error) {
	nbytes, _, _, _, err = c.writeCompressedMessages(codec, msgs...)
	return
}

// WriteCompressedMessagesAt writes a batch of messages to the connection's topic
// and partition, returning the number of bytes written, partition and offset numbers
// and timestamp assigned by the kafka broker to the message set. The write is an atomic
// operation, it either fully succeeds or fails.
//
// If the compression codec is not nil, the messages will be compressed.
func (c *Conn) WriteCompressedMessagesAt(codec CompressionCodec, msgs ...Message) (nbytes int, partition int32, offset int64, appendTime time.Time, err error) {
	return c.writeCompressedMessages(codec, msgs...)
}

func (c *Conn) writeCompressedMessages(codec CompressionCodec, msgs ...Message) (nbytes int, partition int32, offset int64, appendTime time.Time, err error) {

	if len(msgs) == 0 {
		return
	}

	writeTime := time.Now()
	for i, msg := range msgs {
		// users may believe they can set the Topic and/or Partition
		// on the kafka message.
		if msg.Topic != "" && msg.Topic != c.topic {
			err = errInvalidWriteTopic
			return
		}
		if msg.Partition != 0 {
			err = errInvalidWritePartition
			return
		}

		if msg.Time.IsZero() {
			msgs[i].Time = writeTime
		}

		nbytes += len(msg.Key) + len(msg.Value)
	}

	err = c.writeOperation(
		func(deadline time.Time, id int32) error {
			now := time.Now()
			deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
			switch version := c.apiVersions[produceRequest].MaxVersion; {
			case version >= 7:
				return writeProduceRequestV7(
					&c.wbuf,
					codec,
					id,
					c.clientID,
					c.topic,
					c.partition,
					deadlineToTimeout(deadline, now),
					int16(atomic.LoadInt32(&c.requiredAcks)),
					c.transactionalID,
					msgs...,
				)
			case version >= 3:
				return writeProduceRequestV3(
					&c.wbuf,
					codec,
					id,
					c.clientID,
					c.topic,
					c.partition,
					deadlineToTimeout(deadline, now),
					int16(atomic.LoadInt32(&c.requiredAcks)),
					c.transactionalID,
					msgs...,
				)
			default:
				return writeProduceRequestV2(
					&c.wbuf,
					codec,
					id,
					c.clientID,
					c.topic,
					c.partition,
					deadlineToTimeout(deadline, now),
					int16(atomic.LoadInt32(&c.requiredAcks)),
					msgs...,
				)
			}
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(readArrayWith(&c.rbuf, size, func(r *bufio.Reader, size int) (int, error) {
				// Skip the topic, we've produced the message to only one topic,
				// no need to waste resources loading it in memory.
				size, err := discardString(r, size)
				if err != nil {
					return size, err
				}

				// Read the list of partitions, there should be only one since
				// we've produced a message to a single partition.
				size, err = readArrayWith(r, size, func(r *bufio.Reader, size int) (int, error) {
					switch c.produceVersion {
					case v7:
						var p produceResponsePartitionV7
						size, err := p.readFrom(r, size)
						if err == nil && p.ErrorCode != 0 {
							err = Error(p.ErrorCode)
						}
						if err == nil {
							partition = p.Partition
							offset = p.Offset
							appendTime = time.Unix(0, p.Timestamp*int64(time.Millisecond))
						}
						return size, err
					default:
						var p produceResponsePartitionV2
						size, err := p.readFrom(r, size)
						if err == nil && p.ErrorCode != 0 {
							err = Error(p.ErrorCode)
						}
						if err == nil {
							partition = p.Partition
							offset = p.Offset
							appendTime = time.Unix(0, p.Timestamp*int64(time.Millisecond))
						}
						return size, err
					}

				})
				if err != nil {
					return size, err
				}

				// The response is trailed by the throttle time, also skipping
				// since it's not interesting here.
				return discardInt32(r, size)
			}))
		},
	)

	if err != nil {
		nbytes = 0
	}

	return
}

// SetRequiredAcks sets the number of acknowledges from replicas that the
// connection requests when producing messages.
func (c *Conn) SetRequiredAcks(n int) error {
	switch n {
	case -1, 1:
		atomic.StoreInt32(&c.requiredAcks, int32(n))
		return nil
	default:
		return InvalidRequiredAcks
	}
}

func (c *Conn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.rbuf.Peek(8)
	if err != nil {
		return 0, 0, err
	}
	size, id := makeInt32(b[:4]), makeInt32(b[4:])
	return size, id, nil
}

func (c *Conn) skipResponseSizeAndID() {
	c.rbuf.Discard(8)
}

func (c *Conn) readDeadline() time.Time {
	return c.rdeadline.deadline()
}

func (c *Conn) writeDeadline() time.Time {
	return c.wdeadline.deadline()
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (c *Conn) ApiVersions() ([]ApiVersion, error) {
	return c.brokerConn.ApiVersions()
}

// connDeadline is a helper type to implement read/write deadline management on
// the kafka connection.
type connDeadline struct {
	mutex sync.Mutex
	value time.Time
	rconn net.Conn
	wconn net.Conn
}

func (d *connDeadline) deadline() time.Time {
	d.mutex.Lock()
	t := d.value
	d.mutex.Unlock()
	return t
}

func (d *connDeadline) setDeadline(t time.Time) {
	d.mutex.Lock()
	d.value = t

	if d.rconn != nil {
		d.rconn.SetReadDeadline(t)
	}

	if d.wconn != nil {
		d.wconn.SetWriteDeadline(t)
	}

	d.mutex.Unlock()
}

func (d *connDeadline) setConnReadDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.rconn = conn
	d.rconn.SetReadDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) setConnWriteDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.wconn = conn
	d.wconn.SetWriteDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) unsetConnReadDeadline() {
	d.mutex.Lock()
	d.rconn = nil
	d.mutex.Unlock()
}

func (d *connDeadline) unsetConnWriteDeadline() {
	d.mutex.Lock()
	d.wconn = nil
	d.mutex.Unlock()
}

// saslHandshake sends the SASL handshake message.  This will determine whether
// the Mechanism is supported by the cluster.  If it's not, this function will
// error out with UnsupportedSASLMechanism.
//
// If the mechanism is unsupported, the handshake request will reply with the
// list of the cluster's configured mechanisms, which could potentially be used
// to facilitate negotiation.  At the moment, we are not negotiating the
// mechanism as we believe that brokers are usually known to the client, and
// therefore the client should already know which mechanisms are supported.
//
// See http://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
func (c *Conn) saslHandshake(mechanism string) error {
	// The wire format for V0 and V1 is identical, but the version
	// number will affect how the SASL authentication
	// challenge/responses are sent
	var resp saslHandshakeResponseV0
	version := v0
	if c.apiVersions[saslHandshakeRequest].MaxVersion >= 1 {
		version = v1
	}

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.brokerConn.writeRequest(saslHandshakeRequest, version, id, &saslHandshakeRequestV0{Mechanism: mechanism})
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (int, error) {
				return (&resp).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err == nil && resp.ErrorCode != 0 {
		err = Error(resp.ErrorCode)
	}
	return err
}

// saslAuthenticate sends the SASL authenticate message.  This function must
// be immediately preceded by a successful saslHandshake.
//
// See http://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
func (c *Conn) saslAuthenticate(data []byte) ([]byte, error) {
	// if we sent a v1 handshake, then we must encapsulate the authentication
	// request in a saslAuthenticateRequest.  otherwise, we read and write raw
	// bytes.
	if c.apiVersions[saslHandshakeRequest].MaxVersion >= 1 {
		var request = saslAuthenticateRequestV0{Data: data}
		var response saslAuthenticateResponseV0

		err := c.writeOperation(
			func(deadline time.Time, id int32) error {
				return c.writeRequest(saslAuthenticateRequest, v0, id, request)
			},
			func(deadline time.Time, size int) error {
				return expectZeroSize(func() (remain int, err error) {
					return (&response).readFrom(&c.rbuf, size)
				}())
			},
		)
		if err == nil && response.ErrorCode != 0 {
			err = Error(response.ErrorCode)
		}
		return response.Data, err
	}

	// fall back to opaque bytes on the wire.  the broker is expecting these if
	// it just processed a v0 sasl handshake.
	writeInt32(&c.wbuf, int32(len(data)))
	if _, err := c.wbuf.Write(data); err != nil {
		return nil, err
	}
	if err := c.wbuf.Flush(); err != nil {
		return nil, err
	}

	var respLen int32
	_, err := readInt32(&c.rbuf, 4, &respLen)
	if err != nil {
		return nil, err
	}

	resp, _, err := readNewBytes(&c.rbuf, int(respLen), int(respLen))
	return resp, err
}
