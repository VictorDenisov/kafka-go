package kafka

import (
	"bufio"
	"io"
	"net"
	"runtime"
	"sync"
	"time"
)

type BrokerConn struct {
	conn net.Conn

	// deadline management
	wdeadline connDeadline
	rdeadline connDeadline

	// read buffer (synchronized on rlock)
	rlock sync.Mutex
	rbuf  bufio.Reader

	// write buffer (synchronized on wlock)
	wlock sync.Mutex
	wbuf  bufio.Writer

	config BrokerConnConfig

	clientID string

	apiVersions map[apiKey]ApiVersion

	correlationID int32

	fetchVersion   apiVersion
	produceVersion apiVersion
}

type BrokerConnConfig struct {
	ClientID string
}

func NewBrokerConn(conn net.Conn, config BrokerConnConfig) *BrokerConn {
	if len(config.ClientID) == 0 {
		config.ClientID = DefaultClientID
	}

	b := &BrokerConn{
		conn:     conn,
		config:   config,
		clientID: DefaultClientID,
		rbuf:     *bufio.NewReader(conn),
		wbuf:     *bufio.NewWriter(conn),
	}
	b.selectVersions()
	return b
}

func (c *BrokerConn) ApiVersions() ([]ApiVersion, error) {
	id, err := c.doRequest(&c.rdeadline, func(deadline time.Time, id int32) error {
		h := requestHeader{
			ApiKey:        int16(apiVersionsRequest),
			ApiVersion:    int16(v0),
			CorrelationID: id,
			ClientID:      c.clientID,
		}
		h.Size = (h.size() - 4)

		h.writeTo(&c.wbuf)
		return c.wbuf.Flush()
	})
	if err != nil {
		return nil, err
	}

	_, size, lock, err := c.waitResponse(&c.rdeadline, id)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()

	var errorCode int16
	if size, err = readInt16(&c.rbuf, size, &errorCode); err != nil {
		return nil, err
	}
	var arrSize int32
	if size, err = readInt32(&c.rbuf, size, &arrSize); err != nil {
		return nil, err
	}
	r := make([]ApiVersion, arrSize)
	for i := 0; i < int(arrSize); i++ {
		if size, err = readInt16(&c.rbuf, size, &r[i].ApiKey); err != nil {
			return nil, err
		}
		if size, err = readInt16(&c.rbuf, size, &r[i].MinVersion); err != nil {
			return nil, err
		}
		if size, err = readInt16(&c.rbuf, size, &r[i].MaxVersion); err != nil {
			return nil, err
		}
	}

	if errorCode != 0 {
		return r, Error(errorCode)
	}

	return r, nil
}

func (c *BrokerConn) selectVersions() {
	var err error
	apiVersions, err := c.ApiVersions()
	if err != nil {
		c.apiVersions = defaultApiVersions
	} else {
		c.apiVersions = make(map[apiKey]ApiVersion)
		for _, v := range apiVersions {
			c.apiVersions[apiKey(v.ApiKey)] = v
		}
	}
	for _, v := range c.apiVersions {
		if apiKey(v.ApiKey) == fetchRequest {
			switch version := v.MaxVersion; {
			case version >= 10:
				c.fetchVersion = 10
			case version >= 5:
				c.fetchVersion = 5
			default:
				c.fetchVersion = 2
			}
		}
		if apiKey(v.ApiKey) == produceRequest {
			if v.MaxVersion >= 7 {
				c.produceVersion = 7
			} else {
				c.produceVersion = 2
			}
		}
	}
}

func (c *BrokerConn) doRequest(d *connDeadline, write func(time.Time, int32) error) (id int32, err error) {
	c.wlock.Lock()
	c.correlationID++
	id = c.correlationID
	err = write(d.setConnWriteDeadline(c.conn), id)
	d.unsetConnWriteDeadline()

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
	}

	c.wlock.Unlock()
	return
}

func (c *BrokerConn) waitResponse(d *connDeadline, id int32) (deadline time.Time, size int, lock *sync.Mutex, err error) {
	// I applied exactly zero scientific process to choose this value,
	// it seemed to worked fine in practice tho.
	//
	// My guess is 100 iterations where the goroutine gets descheduled
	// by calling runtime.Gosched() may end up on a wait of ~10ms to ~1s
	// (if the programs is heavily CPU bound and has lots of goroutines),
	// so it should allow for bailing quickly without taking too much risk
	// to get false positives.
	const maxAttempts = 100
	var lastID int32

	for attempt := 0; attempt < maxAttempts; {
		var rsz int32
		var rid int32

		c.rlock.Lock()
		deadline = d.setConnReadDeadline(c.conn)

		if rsz, rid, err = c.peekResponseSizeAndID(); err != nil {
			d.unsetConnReadDeadline()
			c.conn.Close()
			c.rlock.Unlock()
			return
		}

		if id == rid {
			c.skipResponseSizeAndID()
			size, lock = int(rsz-4), &c.rlock
			return
		}

		// Optimistically release the read lock if a response has already
		// been received but the current operation is not the target for it.
		c.rlock.Unlock()
		runtime.Gosched()

		// This check is a safety mechanism, if we make too many loop
		// iterations and always draw the same id then we could be facing
		// corrupted data on the wire, or the goroutine(s) sharing ownership
		// of this connection may have panicked and therefore will not be able
		// to participate in consuming bytes from the wire. To prevent entering
		// an infinite loop which reads the same value over and over we bail
		// with the uncommon io.ErrNoProgress error which should give a good
		// enough signal about what is going wrong.
		if rid != lastID {
			attempt++
		} else {
			attempt = 0
		}

		lastID = rid
	}

	err = io.ErrNoProgress
	return
}
func (c *BrokerConn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.rbuf.Peek(8)
	if err != nil {
		return 0, 0, err
	}
	size, id := makeInt32(b[:4]), makeInt32(b[4:])
	return size, id, nil
}

func (c *BrokerConn) skipResponseSizeAndID() {
	c.rbuf.Discard(8)
}

func (c *BrokerConn) readOperation(write func(time.Time, int32) error, read func(time.Time, int) error) error {
	return c.do(&c.rdeadline, write, read)
}

func (c *BrokerConn) writeOperation(write func(time.Time, int32) error, read func(time.Time, int) error) error {
	return c.do(&c.wdeadline, write, read)
}

func (c *BrokerConn) do(d *connDeadline, write func(time.Time, int32) error, read func(time.Time, int) error) error {
	id, err := c.doRequest(d, write)
	if err != nil {
		return err
	}

	deadline, size, lock, err := c.waitResponse(d, id)
	if err != nil {
		return err
	}

	if err = read(deadline, size); err != nil {
		switch err.(type) {
		case error:
		default:
			c.conn.Close()
		}
	}

	d.unsetConnReadDeadline()
	lock.Unlock()
	return err
}

var defaultApiVersions map[apiKey]ApiVersion = map[apiKey]ApiVersion{
	produceRequest:          ApiVersion{int16(produceRequest), int16(v2), int16(v2)},
	fetchRequest:            ApiVersion{int16(fetchRequest), int16(v2), int16(v2)},
	listOffsetRequest:       ApiVersion{int16(listOffsetRequest), int16(v1), int16(v1)},
	metadataRequest:         ApiVersion{int16(metadataRequest), int16(v1), int16(v1)},
	offsetCommitRequest:     ApiVersion{int16(offsetCommitRequest), int16(v2), int16(v2)},
	offsetFetchRequest:      ApiVersion{int16(offsetFetchRequest), int16(v1), int16(v1)},
	groupCoordinatorRequest: ApiVersion{int16(groupCoordinatorRequest), int16(v0), int16(v0)},
	joinGroupRequest:        ApiVersion{int16(joinGroupRequest), int16(v1), int16(v1)},
	heartbeatRequest:        ApiVersion{int16(heartbeatRequest), int16(v0), int16(v0)},
	leaveGroupRequest:       ApiVersion{int16(leaveGroupRequest), int16(v0), int16(v0)},
	syncGroupRequest:        ApiVersion{int16(syncGroupRequest), int16(v0), int16(v0)},
	describeGroupsRequest:   ApiVersion{int16(describeGroupsRequest), int16(v1), int16(v1)},
	listGroupsRequest:       ApiVersion{int16(listGroupsRequest), int16(v1), int16(v1)},
	apiVersionsRequest:      ApiVersion{int16(apiVersionsRequest), int16(v0), int16(v0)},
	createTopicsRequest:     ApiVersion{int16(createTopicsRequest), int16(v0), int16(v0)},
	deleteTopicsRequest:     ApiVersion{int16(deleteTopicsRequest), int16(v1), int16(v1)},
}

// Controller requests kafka for the current controller and returns its URL
func (c *BrokerConn) Controller() (broker Broker, err error) {
	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(metadataRequest, v1, id, topicMetadataRequestV1([]string{}))
		},
		func(deadline time.Time, size int) error {
			var res metadataResponseV1

			if err := c.readResponse(size, &res); err != nil {
				return err
			}
			for _, brokerMeta := range res.Brokers {
				if brokerMeta.NodeID == res.ControllerID {
					broker = Broker{ID: int(brokerMeta.NodeID),
						Port: int(brokerMeta.Port),
						Host: brokerMeta.Host,
						Rack: brokerMeta.Rack}
					break
				}
			}
			return nil
		},
	)
	return broker, err
}

func (c *BrokerConn) writeRequestHeader(apiKey apiKey, apiVersion apiVersion, correlationID int32, size int32) {
	hdr := c.requestHeader(apiKey, apiVersion, correlationID)
	hdr.Size = (hdr.size() + size) - 4
	hdr.writeTo(&c.wbuf)
}

func (c *BrokerConn) writeRequest(apiKey apiKey, apiVersion apiVersion, correlationID int32, req request) error {
	hdr := c.requestHeader(apiKey, apiVersion, correlationID)
	hdr.Size = (hdr.size() + req.size()) - 4
	hdr.writeTo(&c.wbuf)
	req.writeTo(&c.wbuf)
	return c.wbuf.Flush()
}

func (c *BrokerConn) readResponse(size int, res interface{}) error {
	size, err := read(&c.rbuf, size, res)
	switch err.(type) {
	case Error:
		var e error
		if size, e = discardN(&c.rbuf, size, size); e != nil {
			err = e
		}
	}
	return expectZeroSize(size, err)
}

func (c *BrokerConn) requestHeader(apiKey apiKey, apiVersion apiVersion, correlationID int32) requestHeader {
	return requestHeader{
		ApiKey:        int16(apiKey),
		ApiVersion:    int16(apiVersion),
		CorrelationID: correlationID,
		ClientID:      c.clientID,
	}
}

// Brokers retrieve the broker list from the Kafka metadata
func (c *BrokerConn) Brokers() ([]Broker, error) {
	var brokers []Broker
	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(metadataRequest, v1, id, topicMetadataRequestV1([]string{}))
		},
		func(deadline time.Time, size int) error {
			var res metadataResponseV1

			if err := c.readResponse(size, &res); err != nil {
				return err
			}

			brokers = make([]Broker, len(res.Brokers))
			for i, brokerMeta := range res.Brokers {
				brokers[i] = Broker{
					ID:   int(brokerMeta.NodeID),
					Port: int(brokerMeta.Port),
					Host: brokerMeta.Host,
					Rack: brokerMeta.Rack,
				}
			}
			return nil
		},
	)
	return brokers, err
}

// describeGroups retrieves the specified groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
func (c *BrokerConn) describeGroups(request describeGroupsRequestV0) (describeGroupsResponseV0, error) {
	var response describeGroupsResponseV0

	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(describeGroupsRequest, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return describeGroupsResponseV0{}, err
	}
	for _, group := range response.Groups {
		if group.ErrorCode != 0 {
			return describeGroupsResponseV0{}, Error(group.ErrorCode)
		}
	}

	return response, nil
}

// findCoordinator finds the coordinator for the specified group or transaction
//
// See http://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
func (c *BrokerConn) findCoordinator(request findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
	var response findCoordinatorResponseV0

	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(groupCoordinatorRequest, v0, id, request)

		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return findCoordinatorResponseV0{}, err
	}
	if response.ErrorCode != 0 {
		return findCoordinatorResponseV0{}, Error(response.ErrorCode)
	}

	return response, nil
}

// heartbeat sends a heartbeat message required by consumer groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_Heartbeat
func (c *BrokerConn) heartbeat(request heartbeatRequestV0) (heartbeatResponseV0, error) {
	var response heartbeatResponseV0

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(heartbeatRequest, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return heartbeatResponseV0{}, err
	}
	if response.ErrorCode != 0 {
		return heartbeatResponseV0{}, Error(response.ErrorCode)
	}

	return response, nil
}

// joinGroup attempts to join a consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_JoinGroup
func (c *BrokerConn) joinGroup(request joinGroupRequestV1) (joinGroupResponseV1, error) {
	var response joinGroupResponseV1

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(joinGroupRequest, v1, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return joinGroupResponseV1{}, err
	}
	if response.ErrorCode != 0 {
		return joinGroupResponseV1{}, Error(response.ErrorCode)
	}

	return response, nil
}

// leaveGroup leaves the consumer from the consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_LeaveGroup
func (c *BrokerConn) leaveGroup(request leaveGroupRequestV0) (leaveGroupResponseV0, error) {
	var response leaveGroupResponseV0

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(leaveGroupRequest, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return leaveGroupResponseV0{}, err
	}
	if response.ErrorCode != 0 {
		return leaveGroupResponseV0{}, Error(response.ErrorCode)
	}

	return response, nil
}

// listGroups lists all the consumer groups
//
// See http://kafka.apache.org/protocol.html#The_Messages_ListGroups
func (c *BrokerConn) listGroups(request listGroupsRequestV1) (listGroupsResponseV1, error) {
	var response listGroupsResponseV1

	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(listGroupsRequest, v1, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return listGroupsResponseV1{}, err
	}
	if response.ErrorCode != 0 {
		return listGroupsResponseV1{}, Error(response.ErrorCode)
	}

	return response, nil
}

// offsetCommit commits the specified topic partition offsets
//
// See http://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
func (c *BrokerConn) offsetCommit(request offsetCommitRequestV2) (offsetCommitResponseV2, error) {
	var response offsetCommitResponseV2

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(offsetCommitRequest, v2, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return offsetCommitResponseV2{}, err
	}
	for _, r := range response.Responses {
		for _, pr := range r.PartitionResponses {
			if pr.ErrorCode != 0 {
				return offsetCommitResponseV2{}, Error(pr.ErrorCode)
			}
		}
	}

	return response, nil
}

// offsetFetch fetches the offsets for the specified topic partitions.
// -1 indicates that there is no offset saved for the partition.
//
// See http://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
func (c *BrokerConn) offsetFetch(request offsetFetchRequestV1) (offsetFetchResponseV1, error) {
	var response offsetFetchResponseV1

	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(offsetFetchRequest, v1, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return offsetFetchResponseV1{}, err
	}
	for _, r := range response.Responses {
		for _, pr := range r.PartitionResponses {
			if pr.ErrorCode != 0 {
				return offsetFetchResponseV1{}, Error(pr.ErrorCode)
			}
		}
	}

	return response, nil
}

// syncGroups completes the handshake to join a consumer group
//
// See http://kafka.apache.org/protocol.html#The_Messages_SyncGroup
func (c *BrokerConn) syncGroups(request syncGroupRequestV0) (syncGroupResponseV0, error) {
	var response syncGroupResponseV0

	err := c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(syncGroupRequest, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return syncGroupResponseV0{}, err
	}
	if response.ErrorCode != 0 {
		return syncGroupResponseV0{}, Error(response.ErrorCode)
	}

	return response, nil
}

func (c *BrokerConn) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *BrokerConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *BrokerConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
