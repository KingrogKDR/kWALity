package kWALity

import (
	"encoding/json"
	"log"
)

type walEntryPayload struct {
	Lsn           uint64 `json:"lsn"`
	TransactionId int64  `json:"tx_id"`
	Timestamp     int64  `json:"timestamp"`
	Data          []byte `json:"data"`
}

func mustMarshal(entry walEntryPayload) []byte {
	marshaledBytes, err := json.Marshal(entry)
	if err != nil {
		log.Fatalf("Marshaling should never fail: %v", err)
	}
	return marshaledBytes
}
