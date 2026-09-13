package wal

import (
	"errors"
	"fmt"
	"os"
)

type WAL[T any] struct {
	codec          Codec[T]
	currentSegment *os.File
	MaxSegments    uint32
	MaxSegmentSize uint32
	MaxRecordSize  uint32
}

func (w *WAL[T]) Append(data T) error {
	record, err := EncodeRecord(w.codec, data)
	if err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}

	if err := w.writeToSegment(record); err != nil {
		return fmt.Errorf("writing to segment: %w", err)
	}

	return nil
}

func (w *WAL[T]) writeToSegment(record []byte) error {
	// before writing to segment, first check if the segment size exceeds maxSegmentSize
	// 	if noOfSegments > maxSegments, rotate,
	// 	otherwise create a new segment and write to it
	// 	update current segment and then return
	// otherwise write to current segment.
	return nil
}

func (w *WAL[T]) rotateSegment() error {
	return nil
}

func (w *WAL[T]) Sync() error {
	return w.currentSegment.Sync()
}

func (w *WAL[T]) recoverSegment() error {
	info, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil
	}

	var offset int64

	for {
		recordStart := offset

		payload, err := DecodeRecord(w.currentSegment, w.MaxRecordSize)

		switch {
		case errors.Is(err, ErrEndOfRecords):
			return nil
		case errors.Is(err, ErrTruncatedRecord):
			if err := w.currentSegment.Truncate(recordStart); err != nil {
				return fmt.Errorf("truncate incomplete record: %w", err)
			}
			return nil
		case errors.Is(err, ErrChecksumMismatch):
			return fmt.Errorf(
				"corrupt record at offset %d: %w",
				recordStart,
				err,
			)
		case errors.Is(err, ErrInvalidRecordSize):
			return fmt.Errorf(
				"invalid record at offset %d: %w",
				recordStart,
				err,
			)
		case err != nil:
			return fmt.Errorf(
				"reading record at offset %d: %w",
				recordStart,
				err,
			)
		}

		offset += int64(1 + 4 + 4 + len(payload))
	}
}
