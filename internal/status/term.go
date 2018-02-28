// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	gotty "github.com/Nvveen/Gotty"
	"golang.org/x/sys/unix"
)

// Fd() is implemented by *os.File
type fder interface {
	Fd() uintptr
}

// term represents a small set of terminal capabilities used to
// render status updates.
type term struct {
	info *gotty.TermInfo
	fd   uintptr
}

// openTerm attempts to derive a term from the provided
// file descriptor and the $TERM environment variable.
//
// if the file descriptor does not represent a terminal, or
// if the value of $TERM indicates no terminal capabilities
// are present or known, an error is returned.
func openTerm(w io.Writer) (*term, error) {
	fder, ok := w.(fder)
	if !ok {
		return nil, errors.New("cannot get file descriptor from writer")
	}
	if !isTerminal(fder.Fd()) {
		return nil, errors.New("writer is not a terminal")
	}
	switch env := os.Getenv("TERM"); env {
	case "":
		return nil, errors.New("no $TERM defined")
	case "dumb":
		return nil, errors.New("dumb terminal")
	default:
		// In this case, we'll make a best-effort, at least to do vt102.
		t := new(term)
		t.info = safeOpenTermInfo(env)
		t.fd = fder.Fd()
		return t, nil
	}
}

// safeOpenTermInfo wraps gotty's OpenTermInfo to recover from
// panics. We can safely revert to at least vt102 in this case.
func safeOpenTermInfo(env string) (info *gotty.TermInfo) {
	defer func() {
		if recover() != nil {
			info = nil
		}
	}()
	info, _ = gotty.OpenTermInfo(env)
	return
}

func (t *term) print(w io.Writer, which string, params ...interface{}) bool {
	if t.info == nil {
		return false
	}
	attr, err := t.info.Parse(which, params...)
	if err != nil {
		return false
	}
	io.WriteString(w, attr)
	return true
}

// Move moves the terminal cursor by n: if n is negative, we move up
// by -n, when it's positive we move down by n.
func (t *term) Move(w io.Writer, n int) {
	switch {
	case n > 0:
		if t.print(w, "cud", n) {
			return
		}
		fmt.Fprintf(w, "\x1b[%dB", n)
	case n < 0:
		if t.print(w, "cuu", -n) {
			return
		}
		fmt.Fprintf(w, "\x1b[%dA", -n)
	}
}

// Clear clears the current line of the terminal.
func (t *term) Clear(w io.Writer) {
	if !t.print(w, "el1") {
		io.WriteString(w, "\x1b[1K")
	}
	if !t.print(w, "el") {
		fmt.Fprintf(w, "\x1b[K")
	}
}

// Dim returns the current dimensions of the terminal.
func (t *term) Dim() (width, height int) {
	ws, err := getWinsize(t.fd)
	if err != nil {
		return 80, 20
	}
	return int(ws.Width), int(ws.Height)
}

func isTerminal(fd uintptr) bool {
	var t syscall.Termios
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), termios, uintptr(unsafe.Pointer(&t)), 0, 0, 0)
	return err == 0
}

type winsize struct {
	Height, Width uint16
	// We pad the struct to give us plenty of headroom. (In practice,
	// darwin only has 8 additional bytes.) The proper way to do this
	// would be to use cgo to get the proper struct definition, but I'd
	// like to avoid this if possible.
	Pad [128]byte
}

func getWinsize(fd uintptr) (*winsize, error) {
	w := new(winsize)
	_, _, err := unix.Syscall(unix.SYS_IOCTL, fd, uintptr(unix.TIOCGWINSZ), uintptr(unsafe.Pointer(w)))
	if err == 0 {
		return w, nil
	}
	return w, err
}
