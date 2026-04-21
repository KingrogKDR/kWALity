package kWALity

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	segmentPrefix = "segment"
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

type WalOpts struct {
	DirPath     string
	SegmentSize int64
	MaxSegments int
}

func Open(opts WalOpts) (*Wal, error) {
	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Wal{
		dir: opts.DirPath,

		maxSegmentSize: defaultFallback(opts.SegmentSize, 4*1024*1024),
		maxSegments:    defaultFallback(opts.MaxSegments, 10),

		syncTimer: time.NewTimer(syncInterval),
		ctx:       ctx,
		cancel:    cancel,
	}

	pattern := filepath.Join(opts.DirPath, fmt.Sprintf("%s-*", segmentPrefix))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var currentPath string

	if len(matches) == 0 {
		currentPath = filepath.Join(opts.DirPath, fmt.Sprintf("%s-%d.log", segmentPrefix, time.Now().UnixNano()))

		f, err := os.OpenFile(currentPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		w.currentSegment = f
		w.segmentByteOffset = 0
	} else {
		sort.Strings(matches)

		currentPath = matches[len(matches)-1]

		f, err := os.OpenFile(currentPath, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		w.currentSegment = f

		info, err := f.Stat()
		if err != nil {
			return nil, err
		}

		w.segmentByteOffset = uint64(info.Size())
	}

	w.bufWriter = bufio.NewWriter(w.currentSegment)

	if err := w.recover(); err != nil {
		return nil, err
	}

	go w.syncLoop()

	return w, nil
}

func (w *Wal) Append(transactionID int64, data []byte) (uint64, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	return w.writeEntryToBuffer(transactionID, data)
}

func (w *Wal) Sync() {

}

func (w *Wal) Close() {

}
