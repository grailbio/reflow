package reflow

import (
	"fmt"
	"github.com/grailbio/base/digest"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Rundir returns the "runs" directory where reflow run artifacts can be found.
// Rundir returns an error if a it cannot be found (or created).
func Rundir() (string, error) {
	var rundir string
	if home, ok := os.LookupEnv("HOME"); ok {
		rundir = filepath.Join(home, ".reflow", "runs")
		if err := os.MkdirAll(rundir, 0777); err != nil {
			return "", err
		}
	} else {
		var err error
		rundir, err = ioutil.TempDir("", "prefix")
		if err != nil {
			return "", fmt.Errorf("failed to create temporary directory: %v", err)
		}
	}
	return rundir, nil
}

// Runbase returns a base path representing a run with the given id under `Rundir`.
// Runbase should be used to create run-specific artifacts with appropriate file suffix added to the returned base path.
func Runbase(id digest.Digest) (string, error) {
	dir, err := Rundir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, id.Hex()), nil
}
