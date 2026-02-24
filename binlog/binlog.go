package binlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	headerMagic   uint32 = 0xDEADBEEF
	headerVersion uint16 = 1
	defaultNextID uint64 = 10_000
	hdrSize              = 14
	recFixedSize         = 21
)

var le = binary.LittleEndian

func writeHeader(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var hdr [hdrSize]byte

	le.PutUint32(hdr[0:], headerMagic)
	le.PutUint16(hdr[4:], headerVersion)
	le.PutUint64(hdr[6:], defaultNextID)

	n, err := f.Write(hdr[:])
	if err != nil {
		return err
	}
	if n != len(hdr) {
		return io.ErrShortWrite
	}

	return nil
}

func validateHeader(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	hdr := make([]byte, hdrSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return err
	}

	magic := le.Uint32(hdr[:4])
	version := le.Uint16(hdr[4:6])
	if magic != headerMagic {
		return fmt.Errorf("invalid header magic: got 0x%08x want 0x%08x", magic, headerMagic)
	}
	if version != headerVersion {
		return fmt.Errorf("invalid header version: got %d want %d", version, headerVersion)
	}

	return nil
}

func LogInit(f *os.File) error {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var hdr [hdrSize]byte
	n, err := io.ReadFull(f, hdr[:])
	if err != nil {
		// empty file
		if errors.Is(err, io.EOF) && n == 0 {
			return writeHeader(f)
		}

		// corrupt header
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("truncated header: %w", err)
		}

		return fmt.Errorf("read header: %w", err)
	}

	return validateHeader(f)
}
