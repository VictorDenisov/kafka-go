package kafka

import (
	"bufio"
)

type endTransactionRequestV0 struct {
	TransactionalID   string
	ProducerID        int64
	ProducerEpoch     int16
	TransactionResult int8
}

func (t endTransactionRequestV0) size() int32 {
	return sizeof(t.TransactionalID) +
		sizeof(t.ProducerID) +
		sizeof(t.ProducerEpoch) +
		sizeof(t.TransactionResult)
}

func (t endTransactionRequestV0) writeTo(w *bufio.Writer) {
	writeString(w, t.TransactionalID)
	writeInt64(w, t.ProducerID)
	writeInt16(w, t.ProducerEpoch)
	writeInt8(w, t.TransactionResult)
}

type endTransactionResponseV0 struct {
	ThrottleTimeMs int32
	ErrorCode      int16
}

func (t endTransactionResponseV0) size() int32 {
	return sizeof(t.ThrottleTimeMs) + sizeof(t.ErrorCode)
}

func (t endTransactionResponseV0) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMs)
	writeInt16(w, t.ErrorCode)
}

func (t *endTransactionResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMs); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	return
}
