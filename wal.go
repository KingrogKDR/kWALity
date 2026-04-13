package kwality

import (
	"os"
	"sync"
)

type Wal struct {
	log *os.File
	mu  sync.Mutex
	lsn uint64
}

func Open(path string) (*Wal, error)

func (w *Wal) Append()

func (w *Wal) Sync()

func (w *Wal) Close()

func (w *Wal) Replay()
