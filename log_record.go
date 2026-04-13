package kwality

type LogRecord struct {
	LSN      uint64 // log sequence number
	TxnID    uint64 // transaction id
	OldValue []byte
	NewValue []byte
}
