package batch

import (
	"flag"
	"io/ioutil"

	"github.com/grailbio/reflow/errors"
)

func parseFlags(flags *flag.FlagSet, runArgs map[string]string, cmdArgs []string) error {
	// Check that all specified runArgs are valid flags.
	for k := range runArgs {
		if flags.Lookup(k) == nil {
			return errors.New("non-existent flag " + k)
		}
	}

	var err error
	flags.VisitAll(func(f *flag.Flag) {
		if v, ok := runArgs[f.Name]; ok {
			if e := f.Value.Set(v); e != nil {
				err = e
			}
		}
	})
	if err != nil {
		return err
	}

	flags.SetOutput(ioutil.Discard)
	flags.Usage = func() {}
	return flags.Parse(cmdArgs)
}
