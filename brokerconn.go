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

func (c *BrokerConn) writeoperation(write func(time.Time, int32) error, read func(time.Time, int) error) error {
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
