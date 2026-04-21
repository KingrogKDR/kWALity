package kWALity

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

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
	opts := WalOpts{
		DirPath: "test-wal",
	}

	wal, err := Open(opts)
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

	files, _ := filepath.Glob(filepath.Join(opts.DirPath, "segment-*"))
	if len(files) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(files))
	}
}

func TestAppend(t *testing.T) {
	opts := WalOpts{DirPath: "test-wal"}
	wal, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll("test-wal")

	_, err = wal.Append(1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	wal.bufWriter.Flush()

	files, _ := filepath.Glob(filepath.Join(opts.DirPath, "segment-*"))
	entries, err := readAllEntries(files[0])
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}
