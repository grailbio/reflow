// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package execimage

import (
	"crypto"
	_ "crypto/sha256" // Needed for crypto.SHA256
	"debug/elf"
	"debug/macho"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/errors"
)

var digester = digest.Digester(crypto.SHA256)

// ExecPath returns an absolute path to the executable of the current running process.
func ExecPath() (string, error) {
	// TODO(marius): use /proc/self/exe on Linux
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("evalsymlinks: %s %v", path, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute: %s %v", path, err)
	}
	return path, nil
}

// DigestAndSize returns the digest and size of the given Reader.
func DigestAndSize(r io.Reader) (digest.Digest, int64, error) {
	dw := digester.NewWriter()
	size, err := io.Copy(dw, r)
	if err != nil {
		return digest.Digest{}, 0, err
	}
	return dw.Digest(), size, nil
}

func sectionEndAligned(s *elf.Section) uint64 {
	return ((s.Offset + s.FileSize) + (s.Addralign - 1)) & -s.Addralign
}

// ErrNoEmbeddedImage is thrown if the current binary has no embedded linux image.
var ErrNoEmbeddedImage = errors.New("no embedded linux image")

// EmbeddedLinuxImage returns a reader pointing to an embedded linux image
// with the following assumptions:
// - if the current GOOS is linux, returns the current binary.
// - if the current GOOS is darwin, and current binary size is larger
// than what Mach-O reports, returns a reader to the current binary
// offset by the size of the darwin binary.
// - returns ErrNoEmbeddedImage if
//   - if the current GOOS is not darwin
//   - if the current GOOS is darwin, but there's no embedding.
func EmbeddedLinuxImage() (io.ReadCloser, error) {
	path, err := ExecPath()
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "linux" {
		elff, err := elf.Open(path)
		if err != nil {
			return nil, err
		}
		// We could embed a reflowlet in other binaries. This requires us to inspect the binary
		// and find out where the bits of the reflowlet binary start.
		// We read the ELF file sections and determine the section that comes last in the file.
		// The last section's end offset (aligned to the section's alignment) gives us the
		// size of the file.
		var lastOffset uint64
		for _, s := range elff.Sections {
			// section type SHT_NOBITS occupies no space in the file.
			switch {
			case s.Type == elf.SHT_NOBITS:
				continue
			case lastOffset == 0:
				lastOffset = sectionEndAligned(s)
			default:
				offset := sectionEndAligned(s)
				if offset > lastOffset {
					lastOffset = offset
				}
			}
		}
		if lastOffset > uint64(fi.Size()) {
			return nil, errors.New(fmt.Sprintf("ELF file computed size greater than actual size (%v vs %v)", lastOffset, fi.Size()))
		}
		if lastOffset == uint64(fi.Size()) {
			return os.Open(path)
		}
		r, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		if _, err = r.Seek(int64(lastOffset), io.SeekStart); err != nil {
			return nil, err
		}
		return r, nil
	}
	fh, err := macho.Open(path)
	if err != nil {
		return nil, fmt.Errorf("unsupported binary, not mach-o: %v", err)
	}
	sg := fh.Segment("__LINKEDIT")
	machoSize := int64(sg.SegmentHeader.Filesz + sg.SegmentHeader.Offset)
	if fi.Size() == machoSize {
		return nil, ErrNoEmbeddedImage
	}
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if _, err = r.Seek(machoSize, io.SeekStart); err != nil {
		return nil, err
	}
	return r, nil
}
