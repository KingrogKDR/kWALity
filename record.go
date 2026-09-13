package wal

import (
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

const walVersion byte = 1

var recordByteOrder = binary.LittleEndian

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

func EncodeRecord[T any](c Codec[T], data T) ([]byte, error) {
	payload, err := c.Encode(data)
	if err != nil {
		return nil, err
	}

	version := walVersion
	length := uint32(len(payload))
	var lengthBytes []byte
	lengthBytes = recordByteOrder.AppendUint32(lengthBytes, length)
	checksumData := append(lengthBytes, payload...)
	checksum := crc32.Checksum(checksumData, crcTable)
	var record []byte
	record = append(record, version)
	record = append(record, lengthBytes...)
	record = recordByteOrder.AppendUint32(record, checksum)
	record = append(record, payload...)
	return record, nil
}

func DecodeRecord(r io.Reader, maxRecordSize uint32) ([]byte, error) {
	var versionBuf [1]byte
	n, err := io.ReadFull(r, versionBuf[:])
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, ErrEndOfRecords
		}
		return nil, fmt.Errorf("%w: incomplete version", ErrTruncatedRecord)
	}

	var lengthBuf [4]byte
	_, err = io.ReadFull(r, lengthBuf[:])
	if err != nil {
		return nil, fmt.Errorf("%w: incomplete length", ErrTruncatedRecord)
	}

	length := recordByteOrder.Uint32(lengthBuf[:])

	if length > maxRecordSize {
		return nil, fmt.Errorf(
			"%w: record size %d exceeds maximum %d",
			ErrInvalidRecordSize,
			length,
			maxRecordSize,
		)
	}

	var checksumBuf [4]byte
	_, err = io.ReadFull(r, checksumBuf[:])
	if err != nil {
		return nil, fmt.Errorf("%w: incomplete checksum", ErrTruncatedRecord)
	}

	expectedChecksum := recordByteOrder.Uint32(checksumBuf[:])

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
	checksumData := append(lengthBuf[:], payload...)
	actualChecksum := crc32.Checksum(checksumData, crcTable)
	if actualChecksum != expectedChecksum {
		return nil, ErrChecksumMismatch
	}

	return payload, nil
}
