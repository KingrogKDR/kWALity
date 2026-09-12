package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var (
	ErrEndOfRecords      = errors.New("end of records")
	ErrTruncatedRecord   = errors.New("truncated WAL record")
	ErrChecksumMismatch  = errors.New("WAL record checksum mismatch")
	ErrInvalidRecordSize = errors.New("invalid WAL record size")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Transaction uint8

const (
	PutTransaction Transaction = iota + 1
	DeleteTransaction
)

type WALEntry struct {
	transaction Transaction
	data        TransactionData
}

type TransactionData interface {
	isTransaction()
}

type PutData struct {
	key []byte
	val []byte
}

func (PutData) isTransaction() {}

type DeleteData struct {
	key []byte
}

func (DeleteData) isTransaction() {}

func NewWALEntry(data TransactionData) (WALEntry, error) {
	switch d := data.(type) {
	case PutData:
		return WALEntry{
			transaction: PutTransaction,
			data:        PutData{key: bytes.Clone(d.key), val: bytes.Clone(d.val)},
		}, nil
	case DeleteData:
		return WALEntry{
			transaction: DeleteTransaction,
			data:        DeleteData{key: bytes.Clone(d.key)},
		}, nil
	default:
		return WALEntry{}, errors.New("unsupported data")

	}
}

var byteOrder = binary.LittleEndian

func encodeTransaction(entry WALEntry) ([]byte, error) {
	var buf []byte
	switch d := entry.data.(type) {
	case PutData:
		buf = append(buf, byte(PutTransaction))
		buf = byteOrder.AppendUint64(buf, uint64(len(d.key)))
		buf = append(buf, d.key...)
		buf = byteOrder.AppendUint64(buf, uint64(len(d.val)))
		buf = append(buf, d.val...)
	case DeleteData:
		buf = append(buf, byte(DeleteTransaction))
		buf = byteOrder.AppendUint64(buf, uint64(len(d.key)))
		buf = append(buf, d.key...)
	default:
		return nil, errors.New("invalid data for encoding: Requires KEY VALUE for PUT and KEY for DELETE")
	}

	return buf, nil
}

// func decodeTransaction(data []byte) (WALEntry, error) {
// 	if len(data) < 1 {
// 		return WALEntry{}, errors.New("truncated WAL entry: missing WAL data")
// 	}
// 	reader := bytes.NewReader(data)
// 	transactionByte, err := reader.ReadByte()
// 	if err != nil {
// 		return WALEntry{}, fmt.Errorf("truncated WAL entry: missing transaction type: %w", err)
// 	}
// 	transaction := Transaction(transactionByte)

// 	entry := WALEntry{
// 		transaction: transaction,
// 	}

// 	switch transaction {
// 	case PutTransaction:
// 		key, err := readField(reader, "key")
// 		if err != nil {
// 			return WALEntry{}, err
// 		}

// 		val, err := readField(reader, "value")
// 		if err != nil {
// 			return WALEntry{}, err
// 		}
// 		entry.data = PutData{
// 			key: key,
// 			val: val,
// 		}
// 	case DeleteTransaction:
// 		key, err := readField(reader, "key")
// 		if err != nil {
// 			return WALEntry{}, err
// 		}

// 		entry.data = DeleteData{
// 			key: key,
// 		}
// 	default:
// 		return WALEntry{}, errors.New("invalid transaction type")
// 	}

// 	return entry, nil
// }

func readField(reader *bytes.Reader, field string) ([]byte, error) {
	var lenBytes [8]byte
	if _, err := io.ReadFull(reader, lenBytes[:]); err != nil {
		return nil, fmt.Errorf("truncated WAL entry: missing %s length: %w", field, err)
	}
	length := byteOrder.Uint64(lenBytes[:])
	if length > uint64(reader.Len()) {
		return nil, fmt.Errorf("truncated WAL entry: %s length is larger than the rest of the data", field)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("truncated WAL entry: incomplete %s: %w", field, err)
	}
	return data, nil
}

func (e *WALEntry) Encode() ([]byte, error) {
	payload, err := encodeTransaction(*e)
	if err != nil {
		return nil, err
	}

	length := uint64(len(payload))
	checksum := crc32.Checksum(payload, crcTable)
	var record []byte
	record = byteOrder.AppendUint64(record, length)
	record = byteOrder.AppendUint32(record, checksum)
	record = append(record, payload...)
	return record, nil
}

func DecodeRecord(r io.Reader, maxRecordSize uint64) ([]byte, error) {
	var lengthBuf [8]byte
	n, err := io.ReadFull(r, lengthBuf[:])
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, ErrEndOfRecords
		}
		return nil, fmt.Errorf("%w: incomplete length", ErrTruncatedRecord)
	}

	length := byteOrder.Uint64(lengthBuf[:])

	if length > maxRecordSize {
		return nil, fmt.Errorf(
			"%w: record size %d exceeds maximum %d",
			ErrInvalidRecordSize,
			length,
			maxRecordSize,
		)
	}

	var checksumBuf [4]byte
	n, err = io.ReadFull(r, checksumBuf[:])
	if err != nil {
		return nil, fmt.Errorf("%w: incomplete checksum", ErrTruncatedRecord)
	}

	expectedChecksum := byteOrder.Uint32(checksumBuf[:])

	payload := make([]byte, length)
	n, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: expected %d payload bytes, got %d",
			ErrTruncatedRecord,
			length,
			n,
		)
	}

	actualChecksum := crc32.Checksum(payload, crcTable)
	if actualChecksum != expectedChecksum {
		return nil, ErrChecksumMismatch
	}

	return payload, nil
}
