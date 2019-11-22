// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/liveset/bloomlive"
	"github.com/willf/bloom"
)

// If the value does not exist in repository, an error is returned.
func unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, v interface{}) error {
	rc, err := repo.Get(ctx, k)
	if err != nil {
		return err
	}
	defer rc.Close()
	return json.NewDecoder(rc).Decode(v)
}

type filterKind int

const (
	filterAnd filterKind = iota
	filterOr
	filterClause
)

type filter struct {
	kind    filterKind
	clauses []*filter
	re      *regexp.Regexp
	not     bool
}

// Match checks if the set matches any of the filter clauses.
func (f *filter) Match(set []string) bool {
	if f == nil {
		return false
	}
	switch f.kind {
	case filterOr:
		for _, c := range f.clauses {
			if c.Match(set) {
				return true
			}
		}
		return false
	case filterAnd:
		for _, c := range f.clauses {
			if !c.Match(set) {
				return false
			}
		}
		return true
	case filterClause:
		for _, item := range set {
			if f.re.MatchString(item) {
				return !f.not
			}
		}
		return f.not
	}
	panic("bug")
}

func parseFilter(re string) (*filter, error) {
	ors := strings.Split(re, " ")
	f := filter{kind: filterOr, clauses: make([]*filter, len(ors))}
	for i, or := range ors {
		f.clauses[i] = &filter{kind: filterAnd}
		f := f.clauses[i]
		ands := strings.Split(or, ",")
		f.clauses = make([]*filter, len(ands))
		for j, and := range ands {
			not := strings.HasPrefix(and, "!")
			if not {
				and = and[1:]
			}
			re, err := regexp.Compile(and)
			if err != nil {
				return nil, err
			}
			f.clauses[j] = &filter{kind: filterClause, re: re, not: not}
		}
	}
	return &f, nil
}

// mapLiveset implements a liveset.Liveset using a go map.
type mapLiveset map[digest.Digest]struct{}

// Contains tells whether the digest d is definitely in the set.
func (m mapLiveset) Contains(d digest.Digest) bool {
	_, ok := m[d]
	return ok
}

// Add adds the digest to the set.
func (m mapLiveset) Add(d digest.Digest) {
	m[d] = struct{}{}
}

func (c *Cmd) collect(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("collect", flag.ExitOnError)
	thresholdFlag := flags.String("threshold", "YYYY-MM-DD", "cache entries older than this threshold will be collected; supports both date format (YYYY-MM-DD) clause number of days (15d)")
	dryRunFlag := flags.Bool("dry-run", true, "when true, reports on what would have been collected without actually removing anything from the cache")
	rateFlag := flags.Int64("rate", 300, "maximum writes/sec to dynamodb")
	keepFlag := flags.String("keep", "", "regexp to match against labels of cache entries to keep (don't collect)")
	labelsFlag := flags.String("labels", "", "regexp to match against labels of cache entries to collect")
	help := `Collect performs garbage collection of the reflow cache, removing
entries where cache entry labels don't match the keep regexp clause;
and (1) cache entry labels match the labels regexp; or (2) cache
entry has not been accessed more recently than the provided threshold
date.

Keep and label expressions as follows: <clause>[,<clause>,...][
<clause>[,...]...] Space separated clauses are ORed and each OR
clause is an AND of the comma separated sub clauses. A sub clause
preceded by ! is negated.
`

	c.Parse(flags, args, help, "collect [-threshold date] [-keep regexp] [-labels labels]")

	var (
		keepFilter, labelsFilter *filter
		err                      error
	)
	if len(*keepFlag) > 0 {
		keepFilter, err = parseFilter(*keepFlag)
		c.must(err)
	}
	if len(*labelsFlag) > 0 {
		labelsFilter, err = parseFilter(*labelsFlag)
		c.must(err)
	}
	var threshold time.Time
	if strings.HasSuffix(*thresholdFlag, "d") {
		date := time.Now().Local()
		days, err := strconv.Atoi(strings.TrimRight(*thresholdFlag, "d"))
		if err != nil {
			c.Errorln(err)
			flags.Usage()
		}
		threshold = date.AddDate(0, 0, -1*days)
	} else {
		var err error
		threshold, err = time.Parse("2006-01-02", *thresholdFlag)
		if err != nil {
			c.Errorln(err)
			flags.Usage()
		}
	}
	var ass assoc.Assoc
	c.must(c.Config.Instance(&ass))
	var repo reflow.Repository
	c.must(c.Config.Instance(&repo))

	// Use an estimate of the item count in the assoc to create our bloom filters
	count, err := ass.Count(ctx)
	c.Log.Debugf("Finding liveset for cache with %d associations and threshold: %v", count, threshold)

	keyFilter := bloom.NewWithEstimates(uint(count), .000001)
	valueFilter := bloom.NewWithEstimates(uint(count)*10, .000001)

	// Mark for deletion. We need a map based existence filter because we cannot tolerate false positives
	// for deletions.
	deadKeyFilter := make(mapLiveset)
	deadValueFilter := make(mapLiveset)

	// The mapping handler will be call from multiple threads
	var resultsLock sync.Mutex
	itemsScannedCount := int64(0)
	liveItemCount := int64(0)
	liveObjectsInFilesets := int64(0)
	liveObjectsNotInRepository := int64(0)

	start := time.Now()
	err = ass.Scan(ctx, assoc.Fileset, assoc.MappingHandlerFunc(func(k digest.Digest, v []digest.Digest, kind assoc.Kind, lastAccessTime time.Time, labels []string) {
		switch kind {
		case assoc.Fileset:
		default:
			return
		}
		d := v[0]
		checkRepos := func() reflow.Fileset {
			var (
				fs  reflow.Fileset
				err error
			)
			for i := 0; i < 5; i++ {
				err = unmarshal(ctx, repo, d, &fs)
				if err == nil {
					break
				}
				if errors.Is(errors.NotExist, err) {
					// If the object doesn't exist in the repository there's no point adding it to the livesets
					resultsLock.Lock()
					itemsScannedCount++
					liveObjectsNotInRepository++
					resultsLock.Unlock()
					return fs
				}
				if errors.Transient(err) {
					continue
				}
				// If we can't parse the object for another reason bail now
				c.Fatal(fmt.Errorf("error parsing fileset %v (%v)", k, err))
			}
			if err != nil {
				c.Fatal(fmt.Errorf("error parsing fileset %v (%v)", k, err))
			}
			return fs
		}
		live := keepFilter.Match(labels)

		var fs reflow.Fileset
		if !live && labelsFilter.Match(labels) {
			fs = checkRepos()
			resultsLock.Lock()
			defer resultsLock.Unlock()
			for _, f := range fs.Files() {
				deadValueFilter.Add(f.ID)
			}
			deadKeyFilter.Add(k)
			deadValueFilter.Add(d)
			itemsScannedCount++
			if itemsScannedCount%10000 == 0 {
				c.Log.Debugf("Scanned item %d in association", itemsScannedCount)
			}
			return
		}
		live = live || lastAccessTime.After(threshold)
		if live {
			fs = checkRepos()
		}
		// The repository checking happens outside the results lock for better performance
		resultsLock.Lock()
		defer resultsLock.Unlock()
		if live {
			for _, f := range fs.Files() {
				liveObjectsInFilesets++
				valueFilter.Add(f.ID.Bytes())
			}
			keyFilter.Add(k.Bytes())
			valueFilter.Add(d.Bytes())
			liveItemCount++
		}
		itemsScannedCount++
		if itemsScannedCount%10000 == 0 {
			c.Log.Debugf("Scanned item %d in association", itemsScannedCount)
		}
	}))
	// Bail if anything went wrong since we're about to garbage collect based on these livesets
	c.must(err)

	// Some debugging information
	c.Log.Debugf("Time to scan associations %s", time.Since(start))
	c.Log.Printf("Scanned %d associations, found %d live associations, %d live objects, %d objects not in repository",
		itemsScannedCount, liveItemCount, liveObjectsInFilesets, liveObjectsNotInRepository)

	// Garbage collect the repository using the values liveset
	c.must(repo.CollectWithThreshold(ctx, bloomlive.New(valueFilter), deadValueFilter, threshold, *dryRunFlag))

	// Garbage collect the association using the keys liveset
	c.must(ass.CollectWithThreshold(ctx, bloomlive.New(keyFilter), deadKeyFilter, assoc.Fileset, threshold, *rateFlag, *dryRunFlag))
}
