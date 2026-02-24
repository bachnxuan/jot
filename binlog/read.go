package binlog

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func readRecord(f *os.File) (Record, int64, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return Record{}, 0, err
	}

	var buf [recFixedSize]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return Record{}, 0, err
	}

	r := Record{
		Status:    buf[0],
		ID:        le.Uint64(buf[1:9]),
		Timestamp: le.Uint64(buf[9:17]),
	}
	textLen := le.Uint32(buf[17:21])
	text := make([]byte, int(textLen))
	if _, err := io.ReadFull(f, text); err != nil {
		return Record{}, 0, err
	}
	r.Text = text

	return r, offset, nil
}

func seekRecord(f *os.File) error {
	_, err := f.Seek(hdrSize, io.SeekStart)
	return err
}

func ListRecord(f *os.File) ([]Record, error) {
	if err := seekRecord(f); err != nil {
		return nil, err
	}

	records := make([]Record, 0, 256)
	for {
		r, _, err := readRecord(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	return records, nil
}

func SearchRecord(f *os.File, id uint64) (Record, int64, error) {
	if err := seekRecord(f); err != nil {
		return Record{}, 0, err
	}

	var matchedRecord Record
	var offset int64
	found := false
	for {
		var r Record
		var err error
		r, offset, err = readRecord(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Record{}, 0, err
		}
		if r.ID != id {
			continue
		}
		matchedRecord = r
		found = true
		break
	}

	if !found {
		return Record{}, 0, fmt.Errorf("record not found")
	}

	return matchedRecord, offset, nil
}
