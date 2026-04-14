package kwality

import (
	"os"
	"path/filepath"
	"sync"
)

// handles file, handles fsync

type WAL struct {
	file  *os.File
	fName string
	fSize int64
	dir   string

	mu sync.Mutex
}

type WALOpts struct {
	FName string
	Dir   string
}

func NewWAL(opts *WALOpts) (*WAL, error) {
	wal := &WAL{
		fName: opts.FName,
		dir:   opts.Dir,
	}

	return wal, nil
}

func (w *WAL) Open() error {
	err := ensureDir(w.dir)

	if err != nil {
		return err
	}

	fp := filepath.Join(w.dir, w.fName)

	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	w.file = f
	w.fSize = fi.Size()

	return nil
}

func (w *WAL) Append(record *LogRecord) error {
	return nil
}

func (w *WAL) Sync()

func (w *WAL) Close()

func (w *WAL) Replay()
