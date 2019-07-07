package kafka

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"time"
)

type record struct {
	attributes int8
	baseTime   time.Time
	offset     int64
	msg        Message
}

func (r record) size() int32 {
	timestampDelta := r.msg.Time.Sub(r.baseTime)
	offsetDelta := int64(r.offset)

	var res int
	res += 1 + // attributes
		varIntLen(int64(milliseconds(timestampDelta))) +
		varIntLen(offsetDelta) +
		varIntLen(int64(len(r.msg.Key))) +
		len(r.msg.Key) +
		varIntLen(int64(len(r.msg.Value))) +
		len(r.msg.Value) +
		varIntLen(int64(len(r.msg.Headers)))
	for _, h := range r.msg.Headers {
		res += varIntLen(int64(len([]byte(h.Key)))) +
			len([]byte(h.Key)) +
			varIntLen(int64(len(h.Value))) +
			len(h.Value)
	}
	return int32(res)
}

func (r record) writeTo(w *bufio.Writer) {
	timestampDelta := r.msg.Time.Sub(r.baseTime)
	offsetDelta := int64(r.offset)

	writeVarInt(w, int64(r.size()))

	writeInt8(w, r.attributes)
	writeVarInt(w, int64(milliseconds(timestampDelta)))
	writeVarInt(w, offsetDelta)

	writeVarInt(w, int64(len(r.msg.Key)))
	w.Write(r.msg.Key)
	writeVarInt(w, int64(len(r.msg.Value)))
	w.Write(r.msg.Value)
	writeVarInt(w, int64(len(r.msg.Headers)))

	for _, h := range r.msg.Headers {
		writeVarInt(w, int64(len(h.Key)))
		w.Write([]byte(h.Key))
		writeVarInt(w, int64(len(h.Value)))
		w.Write(h.Value)
	}
}

type recordBatchWriter struct {
	codec      CompressionCodec
	attributes int16
	msgs       []Message
	compressed []byte
}

func (r *recordBatchWriter) init() (err error) {
	if r.codec != nil {
		r.attributes = int16(r.codec.Code()) | r.attributes
		recordBuf := &bytes.Buffer{}
		recordBuf.Grow(int(recordBatchSize(r.msgs...)))
		compressedWriter := bufio.NewWriter(recordBuf)
		for i, msg := range r.msgs {
			record{
				attributes: 0,
				baseTime:   r.msgs[0].Time,
				offset:     int64(i),
				msg:        msg,
			}.writeTo(compressedWriter)
		}
		compressedWriter.Flush()

		r.compressed, err = r.codec.Encode(recordBuf.Bytes())
		if err != nil {
			return
		}
		r.attributes = int16(r.codec.Code())
	}
	return nil
}

func (r *recordBatchWriter) size() (size int32) {
	if r.codec != nil {
		size = recordBatchHeaderSize() + int32(len(r.compressed))
	} else {
		size = recordBatchSize(r.msgs...)
	}
	return
}

func recordBatchHeaderSize() int32 {
	return 8 + // base offset
		4 + // batch length
		4 + // partition leader epoch
		1 + // magic
		4 + // crc
		2 + // attributes
		4 + // last offset delta
		8 + // first timestamp
		8 + // max timestamp
		8 + // producer id
		2 + // producer epoch
		4 + // base sequence
		4 // msg count
}

func recordBatchSize(msgs ...Message) (size int32) {
	size = recordBatchHeaderSize()

	baseTime := msgs[0].Time

	for i, msg := range msgs {

		r := record{
			attributes: 0,
			baseTime:   baseTime,
			offset:     int64(i),
			msg:        msg,
		}

		sz := r.size()

		size += r.size() + int32(varIntLen(int64(sz)))
	}
	return
}

func (r *recordBatchWriter) writeTo(w *bufio.Writer) error {
	return writeRecordBatch(
		w,
		r.attributes,
		r.size(),
		func(w *bufio.Writer) {
			if r.codec != nil {
				w.Write(r.compressed)
			} else {
				for i, msg := range r.msgs {
					record{
						attributes: 0,
						baseTime:   r.msgs[0].Time,
						offset:     int64(i),
						msg:        msg,
					}.writeTo(w)
				}
			}

		},
		r.msgs...,
	)
}

func writeRecordBatch(w *bufio.Writer, attributes int16, size int32, write func(*bufio.Writer), msgs ...Message) error {

	baseTime := msgs[0].Time
	lastTime := msgs[len(msgs)-1].Time

	writeInt64(w, int64(0))

	writeInt32(w, int32(size-12)) // 12 = batch length + base offset sizes

	writeInt32(w, -1) // partition leader epoch
	writeInt8(w, 2)   // magic byte

	crcBuf := &bytes.Buffer{}
	crcBuf.Grow(int(size - 12)) // 12 = batch length + base offset sizes
	crcWriter := bufio.NewWriter(crcBuf)

	writeInt16(crcWriter, attributes)         // attributes, timestamp type 0 - create time, not part of a transaction, no control messages
	writeInt32(crcWriter, int32(len(msgs)-1)) // max offset
	writeInt64(crcWriter, timestamp(baseTime))
	writeInt64(crcWriter, timestamp(lastTime))
	writeInt64(crcWriter, -1)               // default producer id for now
	writeInt16(crcWriter, -1)               // default producer epoch for now
	writeInt32(crcWriter, -1)               // default base sequence
	writeInt32(crcWriter, int32(len(msgs))) // record count

	write(crcWriter)
	if err := crcWriter.Flush(); err != nil {
		return err
	}

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	crcChecksum := crc32.Checksum(crcBuf.Bytes(), crcTable)

	writeInt32(w, int32(crcChecksum))
	if _, err := w.Write(crcBuf.Bytes()); err != nil {
		return err
	}

	return nil
}
