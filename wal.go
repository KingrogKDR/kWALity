package kWALity

import (
	"bufio"
	"context"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KingrogKDR/kWALity/internal/record"
)

const (
	segmentPrefix = "segment-"
	syncInterval  = 100 * time.Millisecond
)

type Wal struct {
	dir string

	currentSegment    *os.File
	segmentByteOffset uint64
	maxSegmentSize    int64
	maxSegments       int

	bufWriter *bufio.Writer
	syncTimer *time.Timer
	mut       sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
}

func OpenWal(dirPath string) (*Wal, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	filePath := filepath.Join(dirPath, segmentPrefix)
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &Wal{
		dir: dirPath,

		currentSegment:    f,
		segmentByteOffset: 0,
		maxSegmentSize:    1024,
		maxSegments:       10,

		bufWriter: bufio.NewWriter(f),
		syncTimer: time.NewTimer(syncInterval),
		ctx:       ctx,
		cancel:    cancel,
	}

	return wal, err
}

func (w *Wal) Append(tx_id int64, data []byte) (int, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	return w.writeEntryToBuffer(tx_id, data)
}

func (w *Wal) writeEntryToBuffer(tx_id int64, data []byte) (int, error) {
	entry := &record.WalEntry{
		Lsn:           w.segmentByteOffset,
		TransactionId: tx_id,
		Timestamp:     time.Now(),
		Data:          data,
	}

	encoded := record.MustMarshal(entry)

	entry.CRC = crc32.ChecksumIEEE(encoded)

	finalEntry := record.MustMarshal(entry)

	n, err := w.bufWriter.Write(finalEntry)
	if err != nil {
		return 0, err
	}

	w.segmentByteOffset += uint64(n)

	return n, nil
}
