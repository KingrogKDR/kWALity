package kWALity

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type record struct {
	Lsn           uint64
	TransactionId int64
	Timestamp     int64
	Type          uint16
	DataLen       uint32
	Data          []byte
}

const minPayloadSize = 8 + 8 + 8 + 2 + 4

func (r *record) encode() ([]byte, error) {
	if r.DataLen != uint32(len(r.Data)) {
		return nil, fmt.Errorf("wrong data length")
	}

	payloadLen := minPayloadSize + len(r.Data)
	payload := make([]byte, payloadLen)
	binary.LittleEndian.PutUint64(payload[0:8], r.Lsn)
	binary.LittleEndian.PutUint64(payload[8:16], uint64(r.TransactionId))
	binary.LittleEndian.PutUint64(payload[16:24], uint64(r.Timestamp))
	binary.LittleEndian.PutUint16(payload[24:26], r.Type)
	binary.LittleEndian.PutUint32(payload[26:30], r.DataLen)
	copy(payload[30:], r.Data)

	crc := crc32.NewIEEE()
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(payloadLen))

	crc.Write(lenBuf[:])
	crc.Write(payload)
	checksum := crc.Sum32()

	// Final wal entry layout: [payload_len (4 bytes)][checksum (4 bytes)][payload]
	finalRecord := make([]byte, 4+4+payloadLen)
	binary.LittleEndian.PutUint32(finalRecord[0:4], uint32(payloadLen))
	binary.LittleEndian.PutUint32(finalRecord[4:8], checksum)
	copy(finalRecord[8:], payload)

	return finalRecord, nil
}
