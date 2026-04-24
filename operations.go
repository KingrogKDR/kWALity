package kWALity

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
		return nil, fmt.Errorf("couldn't list segments for rotation: %w", err)
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
	// Case 1:if the no. of segments has crossed max threshold, then replace the oldest segment and
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
			return fmt.Errorf("failed to flush current segment: %w", err)
		}
	}

	if w.currentSegment != nil {
		if err := w.currentSegment.Sync(); err != nil {
			return fmt.Errorf("failed to sync current segment: %w", err)
		}

		if err := w.currentSegment.Close(); err != nil {
			return fmt.Errorf("failed to close current segment: %w", err)
		}
	}

	if removeOldest {
		segs, err := listSegmentsSorted(w.dir)
		if err != nil {
			return err
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

func (w *Wal) writeEntryToBuffer(txID int64, data []byte) (uint64, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	entry := walEntryPayload{
		Lsn:           w.globalLSN,
		TransactionId: txID,
		Data:          data,
		Timestamp:     time.Now().UnixNano(),
	}

	payload := mustMarshal(entry)

	crc := crc32.ChecksumIEEE(payload)

	// Final wal entry layout: [len (4 bytes)][crc (4 bytes)][payload]
	totalLen := 4 + 4 + len(payload)

	if int64(totalLen) > w.maxSegmentSize {
		return 0, fmt.Errorf("wal entry size %d exceeds max segment size %d", totalLen, w.maxSegmentSize)
	}

	// check if remaining segment size is less than the entry size
	// if no, write to buffer
	// if yes, rotate segment:
	remaining := w.maxSegmentSize - int64(w.segmentOffset)
	if remaining < int64(totalLen) {
		matches, err := listSegmentsSorted(w.dir)
		if err != nil {
			return 0, fmt.Errorf("couldn't list segments: %w", err)
		}

		if err := w.rotate(matches); err != nil {
			return 0, err
		}
	}

	buf := make([]byte, totalLen)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc)
	copy(buf[8:], payload)

	n, err := w.bufWriter.Write(buf)
	if err != nil {
		return 0, err
	}

	// update globalLSN and segment offset by adding the bytes written.
	lsn := w.globalLSN
	w.globalLSN += uint64(n)
	w.segmentOffset += uint64(n)

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
