package config

import (
	"fmt"
	"strings"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/pool"
)

func init() {
	Register("labels", "kv", "", "comma separated list of key=value labels",
		func(cfg Config, arg string) (Config, error) {
			l, err := parse(cfg, arg)
			if err != nil {
				return nil, err
			}
			return l, nil
		})
}

type label struct {
	Config
	m pool.Labels
}

func parse(cfg Config, arg string) (Config, error) {
	l := label{cfg, make(pool.Labels)}
	sp := strings.Split(arg, ",")
	for _, v := range sp {
		s := strings.Split(v, "=")
		if len(s) != 2 || len(s[0]) == 0 || len(s[1]) == 0 {
			return nil, errors.E(fmt.Sprintf("invalid label %v", v))
		}
		l.m[s[0]] = s[1]
	}
	return &l, nil
}

func (l *label) Labels() pool.Labels {
	return l.m
}
