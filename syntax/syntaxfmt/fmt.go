package fmt

import "bufio"

// Fmt formats reflow modules.
type Fmt struct {
	w    *writer
	opts Opts
}

// Opts modifies the behavior of Fmt.
type Opts struct {
	// Print debugging information about token positions.
	DebugWritePos bool
	// Formatter will eliminate sequences of blank lines longer than this.
	MaxBlankLines int
}

// New constructs a Fmt that writes to w.
func New(w *bufio.Writer) *Fmt {
	return &Fmt{w: newWriter(w), opts: Opts{MaxBlankLines: 1}}
}

// NewWithOpts constructs a Fmt with opts that writes to w.
func NewWithOpts(w *bufio.Writer, opts Opts) *Fmt {
	return &Fmt{w: newWriter(w), opts: opts}
}
