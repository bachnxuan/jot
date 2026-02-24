package binlog

const (
	StatusActive uint8 = iota
	StatusDeleted
)

type Record struct {
	Status    uint8
	ID        uint64
	Timestamp uint64
	Text      []byte
}
