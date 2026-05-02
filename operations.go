package kWALity

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type segment struct {
	path string
	id   uint64
}

func listAllSegments(dir string) ([]string, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("%s-*", segmentPrefix))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func parseSegmentID(path string) (uint64, error) {
	base := filepath.Base(path) // e.g segment-1.log

	var id uint64
	_, err := fmt.Sscanf(base, "segment-%d.log", &id) // extract 1 from segment-1.log
	return id, err
}

func listSegmentsSorted(dir string) ([]segment, error) {
	paths, err := listAllSegments(dir)
	if err != nil {
		return nil, err
	}

	segs := make([]segment, 0, len(paths))
	for _, p := range paths {
		id, err := parseSegmentID(p)
		if err != nil {
			continue
		}
		segs = append(segs, segment{p, id})
	}

	sort.Slice(segs, func(i, j int) bool {
		return segs[i].id < segs[j].id
	})

	return segs, nil
}

func (w *Wal) createNewSegment() (*os.File, error) {
	currentPath := filepath.Join(w.dir, fmt.Sprintf("%s-%d.log", segmentPrefix, w.nextSegmentID))
	w.nextSegmentID++

	f, err := os.OpenFile(currentPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func getCurrentSegment(segs []segment) (*os.File, error) {
	if len(segs) == 0 {
		return nil, fmt.Errorf("no segments found")
	}

	currentPath := segs[len(segs)-1].path

	f, err := os.OpenFile(currentPath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (w *Wal) rotate(segs []segment) error {
	// Case 1:if the no. of segments has crossed max threshold, then replace the oldest segment
	// Case 2: if not crossed the max threshold, then just create a new one
	shouldRotate := w.maxSegments > 0 && len(segs) >= w.maxSegments
	if shouldRotate {
		if err := w.rotateSegment(true); err != nil {
			return fmt.Errorf("couldn't rotate segment: %w", err)
		}
	} else {
		if err := w.rotateSegment(false); err != nil {
			return fmt.Errorf("couldn't create next segment: %w", err)
		}
	}
	return nil
}

func (w *Wal) rotateSegment(removeOldest bool) error {
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
	}

	if w.currentSegment != nil {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}

		if err := w.currentSegment.Close(); err != nil {
			return err
		}
	}

	if removeOldest {
		segs, err := listSegmentsSorted(w.dir)
		if err != nil {
			return fmt.Errorf("couldn't list segments: %w", err)
		}

		if len(segs) > 0 {
			if err := os.Remove(segs[0].path); err != nil {
				return fmt.Errorf("couldn't remove oldest segment %d: %w", segs[0].id, err)
			}
		}
	}

	newSegment, err := w.createNewSegment()
	if err != nil {
		return fmt.Errorf("couldn't create replacement segment: %w", err)
	}

	w.currentSegment = newSegment
	w.bufWriter = bufio.NewWriter(newSegment)
	w.segmentOffset = 0

	return nil
}

func (w *Wal) writeEntryToBuffer(txID int64, typ uint16, data []byte) (uint64, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	entry := record{
		Lsn:           w.globalLSN,
		TransactionId: txID,
		Timestamp:     time.Now().UnixNano(),
		Type:          typ,
		DataLen:       uint32(len(data)),
		Data:          data,
	}

	recordData, err := entry.encode()
	if err != nil {
		return 0, fmt.Errorf("couldn't encode record: %w", err)
	}

	recordLen := len(recordData)

	if int64(recordLen) > w.maxSegmentSize {
		return 0, fmt.Errorf("wal entry size %d exceeds max segment size %d", recordLen, w.maxSegmentSize)
	}

	// check if remaining segment size is less than the entry size
	// if no, write to buffer
	// if yes, rotate segment:
	remaining := w.maxSegmentSize - int64(w.segmentOffset)
	if remaining < int64(recordLen) {
		matches, err := listSegmentsSorted(w.dir)
		if err != nil {
			return 0, fmt.Errorf("couldn't list segments: %w", err)
		}

		if err := w.rotate(matches); err != nil {
			return 0, err
		}
	}

	err = w.writeFull(recordData)
	if err != nil {
		return 0, err
	}

	// update globalLSN and segment offset
	lsn := w.globalLSN
	w.globalLSN++
	w.segmentOffset += uint64(recordLen)

	return lsn, nil
}

func (w *Wal) writeFull(buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := w.bufWriter.Write(buf[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("buffer writer is stuck: unable to write more bytes")
		}
		written += n
	}
	return nil
}

func (w *Wal) repair() error {
	w.mut.Lock()
	// Example:
	// - scan records
	// - validate checksums
	// - truncate partial/corrupt tail

	return nil
}

func (w *Wal) syncLoop() {
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.Sync(); err != nil {
				log.Printf("Error while performing sync: %v", err)
			}
		}
	}
}
