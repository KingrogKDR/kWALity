package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type WALOptions struct {
	DirPath        string
	MaxSegments    uint32
	MaxSegmentSize uint32
	MaxRecordSize  uint32
}

func DefaultWALOptions() WALOptions {
	return WALOptions{
		DirPath:        ".wal",
		MaxSegments:    10,
		MaxSegmentSize: 4 * 1024 * 1024, // 4MB
		MaxRecordSize:  1 * 1024 * 1024,
	}
}

func mergeDefaults(opts WALOptions) WALOptions {
	defaults := DefaultWALOptions()
	if opts.DirPath != "" {
		defaults.DirPath = opts.DirPath
	}
	if opts.MaxSegments != 0 {
		defaults.MaxSegments = opts.MaxSegments
	}
	if opts.MaxSegmentSize != 0 {
		defaults.MaxSegmentSize = opts.MaxSegmentSize
	}
	if opts.MaxRecordSize != 0 {
		defaults.MaxRecordSize = opts.MaxRecordSize
	}
	return defaults
}

func inspectDir(dirName string) (bool, os.DirEntry, error) {
	f, err := os.Open(dirName)
	if err != nil {
		return false, nil, err
	}
	defer f.Close()

	entries, err := f.ReadDir(-1)
	if err != nil {
		return false, nil, err
	}

	var latest os.DirEntry

	for _, entry := range entries {
		if !isRelevant(entry) {
			continue
		}

		if latest == nil || entry.Name() > latest.Name() {
			latest = entry
		}
	}

	return latest == nil, latest, nil
}

func isRelevant(e os.DirEntry) bool {
	return !e.IsDir() && strings.HasSuffix(e.Name(), ".wlog")
}

func Open[T any](opts WALOptions, codec Codec[T]) (*WAL[T], error) {
	opts = mergeDefaults(opts)
	if err := os.MkdirAll(opts.DirPath, 0o750); err != nil {
		return nil, fmt.Errorf("creating WAL directory: %w", err)
	}

	if opts.MaxRecordSize > opts.MaxSegmentSize {
		return nil, errors.New("max record size exceeds max segment size")
	}

	w := &WAL[T]{
		codec:          codec,
		MaxSegments:    opts.MaxSegments,
		MaxSegmentSize: opts.MaxSegmentSize,
		MaxRecordSize:  opts.MaxRecordSize,
	}

	isEmpty, latestFile, err := inspectDir(opts.DirPath)
	if err != nil {
		return nil, fmt.Errorf("inspecting WAL directory: %w", err)
	}

	var segmentPath string

	if isEmpty {
		segmentPath = filepath.Join(
			opts.DirPath,
			fmt.Sprintf("segment-%06d.wlog", 1),
		)
	} else {
		segmentPath = filepath.Join(opts.DirPath, latestFile.Name())
	}

	segment, err := os.OpenFile(
		segmentPath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0o640,
	)
	if err != nil {
		return nil, fmt.Errorf("opening segment: %w", err)
	}
	w.currentSegment = segment

	if !isEmpty {
		if err := w.recoverSegment(); err != nil {
			segment.Close()
			return nil, fmt.Errorf("recovering segment: %w", err)
		}
	}

	return w, nil
}
