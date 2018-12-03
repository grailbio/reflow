package testutil

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
)

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// FakeContentAt implements io.[Reader|ReaderAt|Seeker|Writer] using a virtual
// file with a predictable pattern. The Read* interfaces will data for the slice
// based on the virtual file containing a repeating pattern containing the
// lowercase alphabet. The WriteAt function will verify that the pattern is
// maintained. This enables unittests with large files without paying the
// performance penalty of disk writes.
type FakeContentAt struct {
	T interface {
		Fatal(...interface{})
	}
	SizeInBytes int64
	Current     int64
	FailureRate float64
}

// Checksum implements ContentAt.
func (fca *FakeContentAt) Checksum() string {
	return "fakechecksum"
}

// Read implements the io.Reader.
func (fca *FakeContentAt) Read(p []byte) (int, error) {
	if fca.Current == fca.SizeInBytes {
		return 0, io.EOF
	}
	if rand.Float64() < fca.FailureRate {
		return 0, errors.New("fake error")
	}

	count := min(int64(len(p)), fca.SizeInBytes-fca.Current)
	for i := int64(0); i < count; i++ {
		p[i] = byte('a' + int((fca.Current+i)%26))
	}
	fca.Current += count

	return int(count), nil
}

// ReadAt implements io.ReaderAt.
func (fca *FakeContentAt) ReadAt(p []byte, off int64) (int, error) {
	if off > fca.SizeInBytes {
		return 0, io.EOF
	}
	if rand.Float64() < fca.FailureRate {
		return 0, errors.New("fake error")
	}

	count := min(fca.SizeInBytes-off, int64(len(p)))

	for i := int64(0); i < count; i++ {
		p[i] = byte('a' + int((off+i)%26))
	}
	return int(count), nil
}

// Seek implements io.Seeker.
func (fca *FakeContentAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		fca.Current = offset
	case 1:
		fca.Current += offset
	case 2:
		fca.Current = fca.Size() + offset
	default:
		panic(fmt.Sprintf("not implemented: %d %d", offset, whence))
	}

	return fca.Current, nil
}

// Write implements io.Writer.
func (fca *FakeContentAt) Write(p []byte) (int, error) {
	if fca.Current+int64(len(p)) > fca.SizeInBytes {
		fca.T.Fatal("write beyond the end of content")
	}

	for i := 0; i < len(p); i++ {
		if p[i] != byte('a'+int((fca.Current+int64(i))%26)) {
			fca.T.Fatal("character mismatch in write")
		}
	}

	fca.Current += int64(len(p))

	return len(p), nil
}

// WriteAt implements io.WriterAt.
func (fca *FakeContentAt) WriteAt(p []byte, off int64) (int, error) {
	for i := 0; i < len(p); i++ {
		if p[i] != byte('a'+int((off+int64(i))%26)) {
			fca.T.Fatal("character mismatch in write")
		}
	}

	return len(p), nil
}

// Size returns the size of the fake content.
func (fca *FakeContentAt) Size() int64 {
	return fca.SizeInBytes
}
