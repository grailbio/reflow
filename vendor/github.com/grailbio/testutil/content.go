package testutil

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
)

// ContentAt allows users of test clients to implement their own content storage.
// This is useful when mocking very large files.
type ContentAt interface {
	io.ReaderAt
	io.WriterAt

	// Size returns the total byte count of the contents.
	Size() int64

	// Checksum returns the checksum of the contents.  It is typically an MD5 hex
	// string, following the S3 convention.
	Checksum() string
}

// ByteContent stores data for content storage tests.
type ByteContent struct {
	Data []byte
}

// ReadAt reads from the specified offset
func (bc *ByteContent) ReadAt(p []byte, off int64) (int, error) {
	reader := bytes.NewReader(bc.Data)
	return reader.ReadAt(p, off)
}

// WriteAt writes at the specified offset
func (bc *ByteContent) WriteAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(len(bc.Data)) {
		tmp := make([]byte, off+int64(len(p)))
		copy(tmp, bc.Data)
		bc.Data = tmp
	}

	copy(bc.Data[off:off+int64(len(p))], p)

	return len(p), nil
}

// Checksum implements ContentAt.
func (bc *ByteContent) Checksum() string {
	return fmt.Sprintf("%x", md5.Sum(bc.Data))
}

// Size returns the size of the contents
func (bc *ByteContent) Size() int64 {
	return int64(len(bc.Data))
}
