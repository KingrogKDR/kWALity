package kWALity

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func serializedEntrySize(txID int64, data []byte) int {
	entry := walEntryPayload{
		Lsn:           0,
		TransactionId: txID,
		Data:          data,
		Timestamp:     time.Now().UnixNano(),
	}

	payload := mustMarshal(entry)
	return 8 + len(payload)
}

func readAllEntries(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries [][]byte

	for {
		header := make([]byte, 8)
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.LittleEndian.Uint32(header[:4])
		expectedCRC := binary.LittleEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		_, err = io.ReadFull(f, payload)
		if err != nil {
			return nil, err
		}

		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("crc mismatch")
		}

		entries = append(entries, payload)
	}

	return entries, nil
}

func TestOpen(t *testing.T) {
	dir := "test-wal"
	wal, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	defer os.RemoveAll("test-wal")

	if wal == nil {
		t.Fatalf("wal is nil")
	}

	if wal.currentSegment == nil {
		t.Fatalf("segment file not initialized")
	}

	files, _ := filepath.Glob(filepath.Join(dir, "segment-*"))
	if len(files) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(files))
	}
}

func TestAppend(t *testing.T) {
	dir := "test-wal"
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll("test-wal")

	_, err = wal.Append(1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	wal.bufWriter.Flush()

	files, _ := filepath.Glob(filepath.Join(dir, "segment-*"))
	entries, err := readAllEntries(files[0])
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestRotateSegment(t *testing.T) {
	dir := t.TempDir()

	wal, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	for range 3 {
		f, err := wal.createNewSegment()
		if err != nil {
			t.Fatalf("failed creating seed segment: %v", err)
		}

		if err := f.Close(); err != nil {
			t.Fatalf("failed closing seed segment: %v", err)
		}

		time.Sleep(time.Millisecond)
	}

	before, err := listSegmentsSorted(dir)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(before) != 4 {
		t.Fatalf("expected 4 segments before rotation, got %d", len(before))
	}

	oldestBefore := before[0]

	if err := wal.rotateSegment(); err != nil {
		t.Fatalf("rotate failed: %v", err)
	}

	after, err := listSegmentsSorted(dir)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(after) != 4 {
		t.Fatalf("expected 4 segments after rotation, got %d", len(after))
	}

	if _, err := os.Stat(oldestBefore.path); !os.IsNotExist(err) {
		t.Fatalf("expected oldest segment %d to be removed", oldestBefore.id)
	}

	foundCurrent := false
	for _, seg := range after {
		if seg.path == wal.currentSegment.Name() {
			foundCurrent = true
			break
		}
	}

	if !foundCurrent {
		t.Fatalf("current segment was not updated to a valid segment file")
	}

	if wal.segmentOffset != 0 {
		t.Fatalf("expected rotated segment offset to reset to 0, got %d", wal.segmentOffset)
	}
}

func TestAppendCreatesNewSegmentWhenLimitReachedAndSpaceInsufficient(t *testing.T) {
	dir := t.TempDir()

	data := make([]byte, 80)
	entrySize := serializedEntrySize(1, data)

	wal, err := Open(
		dir,
		SegmentSizeOpt(int64(entrySize+16)),
		MaxSegmentsOpt(10),
	)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()

	firstSegment := wal.currentSegment.Name()

	if _, err := wal.Append(1, data); err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	if _, err := wal.Append(2, data); err != nil {
		t.Fatalf("second append failed: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(files))
	}

	if _, err := os.Stat(firstSegment); err != nil {
		t.Fatalf("expected original segment to remain: %v", err)
	}

	if wal.currentSegment.Name() == firstSegment {
		t.Fatalf("expected current segment to advance to a new file")
	}
}

func TestAppendRotatesWhenMaxSegmentsReachedAndSpaceInsufficient(t *testing.T) {
	dir := t.TempDir()

	data := make([]byte, 80)

	entrySize := serializedEntrySize(1, data)

	wal, err := Open(
		dir,
		SegmentSizeOpt(int64(entrySize+16)),
		MaxSegmentsOpt(4),
	)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer wal.Close()
	for range 3 {
		f, err := wal.createNewSegment()
		if err != nil {
			t.Fatalf("failed creating seed segment: %v", err)
		}

		if err := f.Close(); err != nil {
			t.Fatalf("failed closing seed segment: %v", err)
		}

		time.Sleep(time.Millisecond)
	}

	before, err := listSegmentsSorted(dir)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	oldestBefore := before[0]

	if _, err := wal.Append(1, data); err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	if _, err := wal.Append(2, data); err != nil {
		t.Fatalf("second append failed: %v", err)
	}

	after, err := listSegmentsSorted(dir)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(after) != 4 {
		t.Fatalf("expected segment count to stay at max (4), got %d", len(after))
	}

	if _, err := os.Stat(oldestBefore.path); !os.IsNotExist(err) {
		t.Fatalf("expected oldest segment %d to be removed", oldestBefore.id)
	}
}
