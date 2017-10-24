package file

import (
	"os"
	"path/filepath"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	fswalker "github.com/grailbio/reflow/internal/walker"
)

type walker struct {
	repo   *Repository
	walker fswalker.Walker
	dgst   digest.Digest
	err    error
}

func (w *walker) Init(r *Repository) {
	w.walker.Init(r.Root)
}

func (w *walker) Digest() digest.Digest {
	return w.dgst
}

func (w *walker) Err() error {
	if w.err != nil {
		return w.err
	}
	return w.walker.Err()
}

func (w *walker) Path() string {
	return w.walker.Path()
}

func (w *walker) Info() os.FileInfo {
	return w.walker.Info()
}

func (w *walker) Scan() bool {
	for {
		if !w.walker.Scan() {
			return false
		}
		if w.walker.Info().IsDir() {
			continue
		}
		path := w.Path()
		first, last := filepath.Base(filepath.Dir(path)), filepath.Base(path)
		if first == "tmp" {
			continue
		}
		w.dgst, w.err = reflow.Digester.Parse(first + last)
		if w.err != nil {
			return false
		}
		return true
	}
}
