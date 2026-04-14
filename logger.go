package kwality

type LogRecord struct {
	LSN      uint64 // log sequence number
	TxnID    uint64 // transaction id
	OldValue []byte
	NewValue []byte
}

type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

type Logger interface {
	Log(level Level, msg string, fields map[string]any) error
}

type DefaultLogger struct {
	wal *WAL
}

func NewDefaultLogger(w *WAL) *DefaultLogger {
	return &DefaultLogger{wal: w}
}

func (l *DefaultLogger) Log(level Level, msg string, fields map[string]any) error {
	record := &LogRecord{
		LSN:      0, // you will manage later
		TxnID:    0,
		OldValue: nil,
		NewValue: []byte(msg), // simple for now
	}

	// encode record

	return l.wal.Append(record)
}
