package record

import (
	"encoding/json"
	"log"
	"time"
)

type WalEntry struct {
	Lsn           uint64    `json:"lsn"`
	TransactionId int64     `json:"tx_id"`
	Timestamp     time.Time `json:"timestamp"`
	Data          []byte    `json:"data"`
	CRC           uint32    `json:"checksum"`
}

func MustMarshal(entry *WalEntry) []byte {
	marshaledBytes, err := json.Marshal(entry)
	if err != nil {
		log.Fatalf("Marshaling should never fail: %v", err)
	}
	return marshaledBytes
}
