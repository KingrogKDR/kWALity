package kWALity

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	segmentPrefix = "segment"
)

type Wal struct {
	dir string

	globalLSN      uint64
	nextSegmentID  uint64
	currentSegment *os.File
	maxSegmentSize int64
	maxSegments    int
	segmentOffset  uint64

	bufWriter    *bufio.Writer
	syncInterval time.Duration
	mut          sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

type Option func(w *Wal)

func SegmentSizeOpt(size int64) Option {
	return func(w *Wal) {
		w.maxSegmentSize = size
	}
}

func MaxSegmentsOpt(n int) Option {
	return func(w *Wal) {
		w.maxSegments = n
	}
}

func SyncIntervalOpt(d time.Duration) Option {
	return func(w *Wal) {
		w.syncInterval = d
	}
}

func Open(dir string, opts ...Option) (*Wal, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("couldn't create directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Wal{
		dir: dir,

		globalLSN:      0,
		maxSegmentSize: 4 * 1024 * 1024,
		maxSegments:    10,

		syncInterval: 100 * time.Millisecond,
		ctx:          ctx,
		cancel:       cancel,
	}

	// apply custom options
	for _, opt := range opts {
		opt(w)
	}

	// match for all sorted segments in the directory
	segs, err := listSegmentsSorted(dir)
	if err != nil {
		return nil, fmt.Errorf("couldn't list segments: %w", err)
	}

	// create a new segment if none is present, otherwise get the current segment and mark its offset
	if len(segs) == 0 {
		w.nextSegmentID = 1
		f, err := w.createNewSegment()

		if err != nil {
			return nil, fmt.Errorf("couldn't create new segment: %w", err)
		}

		w.currentSegment = f
		w.segmentOffset = 0
	} else {
		w.nextSegmentID = segs[len(segs)-1].id + 1

		f, err := getCurrentSegment(segs)

		if err != nil {
			return nil, fmt.Errorf("couldn't get current segment: %w", err)
		}

		w.currentSegment = f

		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed getting current segment info: %w", err)
		}
		w.segmentOffset = uint64(info.Size())
	}

	w.bufWriter = bufio.NewWriter(w.currentSegment)

	// repair/recover the segment before syncing
	if err := w.recover(); err != nil {
		return nil, fmt.Errorf("couldn't recover wal: %w", err)
	}

	go w.syncLoop()

	return w, nil
}

func (w *Wal) Append(transactionID int64, data []byte) (uint64, error) {
	lsn, err := w.writeEntryToBuffer(transactionID, data)

	if err != nil {
		return 0, fmt.Errorf("couldn't write wal entry to buffer: %w", err)
	}

	return lsn, nil
}

func (w *Wal) Sync() error {
	w.mut.Lock()
	defer w.mut.Unlock()

	if w.bufWriter != nil {
		err := w.bufWriter.Flush()
		if err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	if w.currentSegment != nil {
		err := w.currentSegment.Sync()
		if err != nil {
			return fmt.Errorf("fsync failed: %w", err)
		}
	}

	return nil
}

func (w *Wal) Close() error {
	w.cancel()
	if err := w.Sync(); err != nil {
		return err
	}

	return w.currentSegment.Close()
}
