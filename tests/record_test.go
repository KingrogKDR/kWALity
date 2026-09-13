package tests

import (
	"bytes"
	"testing"

	wal "github.com/KingrogKDR/kWALity"
)

type stringCode struct{}

func (stringCode) Encode(data string) ([]byte, error) {
	return []byte(data), nil
}

func (stringCode) Decode(data []byte) (string, error) {
	return string(data), nil
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	codec := stringCode{}
	original := "hello"
	record, err := wal.EncodeRecord(codec, original)
	if err != nil {
		t.Fatalf("EncodeRecord() error = %v", err)
	}

	payload, err := wal.DecodeRecord(bytes.NewReader(record), 1024)
	if err != nil {
		t.Fatalf("DecodeRecord() error = %v", err)
	}

	decoded, err := codec.Decode(payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if decoded != original {
		t.Fatalf("decoded = %q, want %q", decoded, original)
	}
}
