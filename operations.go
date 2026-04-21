package kWALity

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

func defaultFallback[T comparable](val, def T) T {
	var zero T
	if val == zero {
		return def
	}
	return val
}

func (w *Wal) writeEntryToBuffer(txID int64, data []byte) (uint64, error) {
	entry := walEntryPayload{
		Lsn:           w.segmentByteOffset,
		TransactionId: txID,
		Timestamp:     time.Now().UnixNano(),
		Data:          data,
	}

	payload := mustMarshal(entry)

	crc := crc32.ChecksumIEEE(payload)

	// Final wal entry layout: [len][crc][payload]
	totalLen := 4 + 4 + len(payload)
	buf := make([]byte, totalLen)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc)
	copy(buf[8:], payload)

	n, err := w.bufWriter.Write(buf)
	if err != nil {
		return 0, err
	}

	lsn := w.segmentByteOffset
	w.segmentByteOffset += uint64(n)

	return lsn, nil
}

func (w *Wal) recover() error {
	// Example:
	// - scan records
	// - validate checksums
	// - truncate partial/corrupt tail

	return nil
}

func (w *Wal) syncLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.syncTimer.C:
			w.mut.Lock()
			if w.bufWriter != nil {
				w.bufWriter.Flush()
			}
			if w.currentSegment != nil {
				w.currentSegment.Sync()
			}
			w.mut.Unlock()

			w.syncTimer.Reset(syncInterval)
		}
	}
}
