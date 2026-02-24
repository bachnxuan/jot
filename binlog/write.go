package binlog

import (
	"fmt"
	"io"
	"os"
	"time"
)

func allocID(f *os.File) (uint64, error) {
	var buf [8]byte

	if _, err := f.Seek(6, io.SeekStart); err != nil {
		return 0, err
	}

	// read next_id
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return 0, err
	}
	cur := le.Uint64(buf[:])

	// update next_id
	le.PutUint64(buf[:], cur+1)
	if _, err := f.Seek(6, io.SeekStart); err != nil {
		return 0, err
	}

	n, err := f.Write(buf[:])
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return 0, io.ErrShortWrite
	}

	return cur, nil
}

func CreateRecord(f *os.File, text []byte) (Record, error) {
	timestamp := uint64(time.Now().Unix())
	id, err := allocID(f)
	if err != nil {
		return Record{}, err
	}
	newRecord := Record{
		Status:    StatusActive,
		Timestamp: timestamp,
		ID:        id,
		Text:      text,
	}

	return newRecord, nil
}

func AppendRecord(f *os.File, r *Record) error {
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	textLen := uint32(len(r.Text))
	buf := make([]byte, recFixedSize+int(textLen))

	buf[0] = r.Status
	le.PutUint64(buf[1:9], r.ID)
	le.PutUint64(buf[9:17], r.Timestamp)
	le.PutUint32(buf[17:21], textLen)

	if textLen > 1024*1024 { // 1 MiB
		return fmt.Errorf("text too large: %d", textLen)
	}
	copy(buf[21:], r.Text)

	n, err := f.Write(buf[:])
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func RemoveRecord(f *os.File, id uint64) error {
	_, offset, err := SearchRecord(f, id)
	if err != nil {
		return err
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	var buf [1]byte
	buf[0] = StatusDeleted
	n, err := f.Write(buf[:])
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}

	return nil
}
