// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
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
	"github.com/grailbio/reflow/repository"
	"github.com/willf/bloom"
)

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

type collectInputs struct {
	valueFilter, keyFilter         *bloom.BloomFilter
	deadValueFilter, deadKeyFilter mapLiveset

	itemsScannedCount          int64
	itemsMigratedCount         int64
	itemsMigratedAttemptCount  int64
	liveItemCount              int64
	liveObjectsInFilesets      int64
	liveObjectsNotInRepository int64
}

func (c *Cmd) buildCollectInputsAndMigrate(ctx context.Context, ass assoc.Assoc, repo reflow.Repository,
	keepFilter, labelsFilter *filter, threshold time.Time, migrateFS2MaxAttemptsCount int64) (*collectInputs, error) {
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
	itemsMigratedCount := int64(0)
	itemsMigratedAttemptCount := int64(0)

	mappingHandlerFn := func(k digest.Digest, v map[assoc.Kind]digest.Digest, lastAccessTime time.Time, labels []string) {
		var fs reflow.Fileset
		for kind, d := range v {
			switch kind {
			case assoc.Fileset, assoc.FilesetV2:
			default:
				return
			}

			checkRepos := func(kind assoc.Kind) (reflow.Fileset, error) {
				var (
					crFs  reflow.Fileset
					crErr error
				)
				for i := 0; i < 5; i++ {
					crErr = repository.Unmarshal(ctx, repo, d, &crFs, kind)
					if crErr == nil {
						break
					}
					if errors.Is(errors.NotExist, crErr) {
						// If the object doesn't exist in the repository there's no point adding it to the livesets
						resultsLock.Lock()
						itemsScannedCount++
						liveObjectsNotInRepository++
						resultsLock.Unlock()
						return crFs, nil
					}
					if errors.Transient(crErr) {
						continue
					}
					// If we can't parse the object for another reason bail now
					return reflow.Fileset{}, fmt.Errorf("%s %v (flow %v): %v", kind.String(), d, k, crErr)
				}
				if crErr != nil {
					return reflow.Fileset{}, fmt.Errorf("fileset %v (flow %v): %v", d, k, crErr)
				}
				return crFs, nil
			}

			live := keepFilter.Match(labels)
			if !live && labelsFilter.Match(labels) {
				fs, err = checkRepos(kind)
				if err != nil {
					c.Log.Error(err)
					return
				}
				resultsLock.Lock()
				for _, f := range fs.Files() {
					deadValueFilter.Add(f.ID)
				}
				deadKeyFilter.Add(k)
				deadValueFilter.Add(d)
				itemsScannedCount++
				if itemsScannedCount%10000 == 0 {
					c.Log.Debugf("Scanned item %d in association", itemsScannedCount)
				}
				resultsLock.Unlock()
				return
			}
			live = live || lastAccessTime.After(threshold)
			if live {
				fs, err = checkRepos(kind)
				if err != nil {
					c.Log.Error(err)
					return
				}
			}
			// The repository checking happens outside the results lock for better performance
			resultsLock.Lock()
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
			resultsLock.Unlock()
		}

		// check if assoc key has a v1 fileset that require migrations
		_, fsOk := v[assoc.Fileset]
		_, fs2Ok := v[assoc.FilesetV2]
		resultsLock.Lock()
		if fsOk && !fs2Ok && (migrateFS2MaxAttemptsCount == -1 || itemsMigratedAttemptCount < migrateFS2MaxAttemptsCount) {
			itemsMigratedAttemptCount++
			resultsLock.Unlock()
			var mErr error
			var fsid digest.Digest
			if fsid, mErr = repository.Marshal(ctx, repo, fs); mErr != nil {
				c.Log.Errorf("Failed to marshal during filesetV2 migration [k=%v]: %s", k, mErr)
				return
			}
			if mErr = ass.Store(context.Background(), assoc.FilesetV2, k, fsid); mErr != nil {
				c.Log.Errorf("Failed to store in assoc during filesetV2 migration [%v=%v]: %s", k, fsid, mErr)
				return
			}
			resultsLock.Lock()
			itemsMigratedCount++
		}
		resultsLock.Unlock()
	}

	err = ass.Scan(ctx, []assoc.Kind{assoc.Fileset, assoc.FilesetV2}, assoc.MappingHandlerFunc(mappingHandlerFn))
	inps := &collectInputs{
		valueFilter:                valueFilter,
		keyFilter:                  keyFilter,
		deadValueFilter:            deadValueFilter,
		deadKeyFilter:              deadKeyFilter,
		itemsScannedCount:          itemsScannedCount,
		itemsMigratedCount:         itemsMigratedCount,
		itemsMigratedAttemptCount:  itemsMigratedAttemptCount,
		liveItemCount:              liveItemCount,
		liveObjectsInFilesets:      liveObjectsInFilesets,
		liveObjectsNotInRepository: liveObjectsNotInRepository,
	}
	return inps, err
}

func (c *Cmd) collect(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("collect", flag.ExitOnError)
	thresholdFlag := flags.String("threshold", "YYYY-MM-DD", "cache entries older than this threshold will be collected; supports both date format (YYYY-MM-DD) clause number of days (15d)")
	dryRunFlag := flags.Bool("dry-run", true, "when true, reports on what would have been collected without actually removing anything from the cache")
	rateFlag := flags.Int64("rate", 300, "maximum writes/sec to dynamodb")
	keepFlag := flags.String("keep", "", "regexp to match against labels of cache entries to keep (don't collect)")
	labelsFlag := flags.String("labels", "", "regexp to match against labels of cache entries to collect")
	migrateFS2MaxAttemptsFlag := flags.Int64("migrate-fs-max-attempts", 5000, "max count of v1 filesets to attempt to migrate to the v2 format during each run (0=none, -1=no limit)")
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

	start := time.Now()
	var inps *collectInputs
	inps, err = c.buildCollectInputsAndMigrate(ctx, ass, repo, keepFilter,
		labelsFilter, threshold, *migrateFS2MaxAttemptsFlag)
	// Bail if anything went wrong since we're about to garbage collect based on these livesets
	c.must(err)

	// Some debugging information
	c.Log.Debugf("Time to scan associations %s", time.Since(start))
	c.Log.Printf("Scanned %d associations, found %d live associations, %d live objects, %d objects not in repository",
		inps.itemsScannedCount, inps.liveItemCount, inps.liveObjectsInFilesets, inps.liveObjectsNotInRepository)

	// Garbage collect the repository using the values liveset
	c.must(repo.CollectWithThreshold(ctx, bloomlive.New(inps.valueFilter), inps.deadValueFilter, threshold, *dryRunFlag))

	// Garbage collect the association using the keys liveset
	c.must(ass.CollectWithThreshold(ctx, bloomlive.New(inps.keyFilter), inps.deadKeyFilter, threshold, *rateFlag, *dryRunFlag))
}
