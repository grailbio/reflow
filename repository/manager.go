// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"golang.org/x/sync/errgroup"
)

// Limits stores a default limits and maintains a set of overrides by
// key.
type Limits struct {
	def    int
	limits map[string]int
}

// NewLimits returns a new Limits with a default value.
func NewLimits(def int) *Limits {
	return &Limits{def: def, limits: map[string]int{}}
}

func (l *Limits) String() string {
	return fmt.Sprintf("limits{def:%d overrides:%v}", l.def, l.limits)
}

// Set sets limit v for key k.
func (l *Limits) Set(k string, v int) {
	l.limits[k] = v
}

// Limit retrieves the limit for key k.
func (l *Limits) Limit(k string) int {
	if n, ok := l.limits[k]; ok {
		return n
	}
	return l.def
}

type transferStatus int

const (
	// Waiting indicates that the transfer is queued.
	waiting transferStatus = iota
	// Transferring indicates that the transfer is currently being performed.
	transferring
	// Done indicates the transfer is complete.
	done
	maxStatus
)

// Stat stores file transfer statistics.
type stat struct {
	// Size is the total size of all the files in the transfer.
	Size int64
	// N is the number of files in the transfer.
	N int64
}

func (s stat) String() string {
	return fmt.Sprintf("%d %s", s.N, data.Size(s.Size))
}

// Sub returns the stat of s subtracted by t.
func (s stat) Sub(t stat) stat {
	return stat{
		Size: s.Size - t.Size,
		N:    s.N - t.N,
	}
}

// Add returns the state of s added to t.
func (s stat) Add(t stat) stat {
	return stat{
		Size: s.Size + t.Size,
		N:    s.N + t.N,
	}
}

// IsZero tells whether stat s is zero.
func (s stat) IsZero() bool {
	return s.Size == 0 && s.N == 0
}

// TransferStats keep track of transfer stats by status.
type transferStat [maxStatus]stat

// Pending returns whether this stat has any non-done entries.
func (t *transferStat) Pending() bool {
	return (*t)[waiting].IsZero() && (*t)[transferring].IsZero()
}

// Update updates the transferStat t with stat in the given status.
//
// TODO(marius): report transfer rates also
func (t *transferStat) Update(status transferStatus, stat stat) {
	if status > waiting {
		(*t)[status-1] = (*t)[status-1].Sub(stat)
	}
	(*t)[status] = (*t)[status].Add(stat)
}

func (t transferStat) String() string {
	return fmt.Sprintf("done: %s, transferring: %s, waiting: %s", t[done], t[transferring], t[waiting])
}

// Task represents a single transfer task. It is used to
// provide a status.Task for a single transfer.
type task struct {
	*status.Task
	transferStat
}

// A transfer represents a single file transfer from a source
// to a destination repository.
type transfer struct {
	Err error
	C   chan struct{}
}

type transferKey struct {
	Dest   string
	FileID digest.Digest
}

// A Manager is used to transfer objects between repositories while
// enforcing transfer policies.
//
// BUG(marius): Manager does not release references to repositories;
// in long-term processes, this could cause space leaks.
type Manager struct {
	// Log is used to report manager status.
	Log *log.Logger

	// PendingTransfers defines limits for the number of outstanding
	// transfers a repository (keyed by URL) may have in flight.
	// The limit is used separately for transmit and receive traffic.
	// It is not possible currently to set different limits for the two
	// directions.
	PendingTransfers *Limits

	// Stat defines limits for the number of stat operations that
	// may be issued concurrently to any given repository.
	Stat *Limits

	// Status is used to report active transfers to.
	Status *status.Group

	mu sync.Mutex

	src, dst, stat map[string]*limiter.Limiter

	// tasks represents the current transfer tasks, rolled up by src->dst.
	tasks map[string]*task

	managerStat transferStat

	transfers map[transferKey]*transfer
	files     map[digest.Digest]reflow.File
}

// Transfer transmits a set of files between two repositories,
// subject to policies. Files that already exist in the destination
// repository are skipped.
//
// TODO(marius): we may want to consider single-flighting download
// requests by sha. At least for large objects.
func (m *Manager) Transfer(ctx context.Context, dst, src reflow.Repository, files ...reflow.File) error {
	var err error
	files, err = m.NeedTransfer(ctx, dst, files...)
	if err != nil {
		return err
	}
	if src == dst {
		if len(files) > 0 {
			return errors.E(errors.NotExist, errors.Errorf("missing %d files", len(files)))
		}
		return nil
	}
	return m.transfer(ctx, dst, src, files...)
}

// NeedTransfer determines which of the provided files are missing from
// the destination repository and must therefore be transfered.
func (m *Manager) NeedTransfer(ctx context.Context, dst reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	exists := make([]bool, len(files))
	lstat := m.limiter(dst, &m.stat, m.Stat)
	g, gctx := errgroup.WithContext(ctx)
	for _, file := range files {
		if file.IsRef() {
			return nil, errors.E("needtransfer", errors.Invalid, errors.Errorf("unresolved file: %v", file))
		}
	}
	for i, file := range files {
		if err := lstat.Acquire(gctx, 1); err != nil {
			return nil, err
		}
		i, file := i, file
		// TODO(marius): implement a batch stat call.
		// It will be more efficient in most cases.
		g.Go(func() error {
			_, err := dst.Stat(ctx, file.ID)
			lstat.Release(1)
			exists[i] = err == nil
			if err != nil && !errors.Is(errors.NotExist, err) {
				m.Log.Printf("stat %v %v: %v", dst, file.ID, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	all := files
	files = nil
	for i := range exists {
		if !exists[i] {
			files = append(files, all[i])
		}
	}
	return files, nil
}

func (m *Manager) transfer(ctx context.Context, dst, src reflow.Repository, files ...reflow.File) error {
	var (
		lx = m.limiter(dst, &m.dst, m.PendingTransfers)
		ly = m.limiter(src, &m.src, m.PendingTransfers)
		ux = key(dst)
		uy = key(src)
	)
	if uy < ux {
		ux, uy = uy, ux
		lx, ly = ly, lx
	}
	var total stat
	for _, file := range files {
		total.Size += file.Size
		total.N++
	}
	start := time.Now()
	g1, g1ctx := errgroup.WithContext(ctx)
	g2, g2ctx := errgroup.WithContext(ctx)
	for i := range files {
		file := files[i]
		transfer, claimed := m.claim(dst, src, file)
		if !claimed {
			g2.Go(func() error {
				// TODO(marius): this approach may tie together unrelated
				// contexts; if one is cancelled, the other dependent transfers
				// are also cancelled even though their contexts would permit the
				// computation to proceed. Fix this.
				select {
				case <-transfer.C:
					return transfer.Err
				case <-g2ctx.Done():
					return g2ctx.Err()
				}
			})
			continue
		}
		m.updateStats(src, dst, waiting, stat{file.Size, 1})
		if err := lx.Acquire(g1ctx, 1); err != nil {
			m.done(dst, src, file, err)
			if err != nil {
				return err
			}
			return err
		}
		if err := ly.Acquire(g1ctx, 1); err != nil {
			lx.Release(1)
			m.done(dst, src, file, err)
			if err != nil {
				return err
			}
			return err
		}
		g1.Go(func() error {
			stat := stat{file.Size, 1}
			m.updateStats(src, dst, transferring, stat)
			err := Transfer(g1ctx, dst, src, file.ID)
			if err != nil {
				err = errors.E("transfer", file.ID, err)
			}
			m.updateStats(src, dst, done, stat)
			ly.Release(1)
			lx.Release(1)
			m.done(dst, src, file, err)
			return err
		})
	}
	err := g1.Wait()
	if err != nil {
		return err
	}
	err = g2.Wait()
	if err != nil {
		return err
	}
	dur := time.Since(start)
	if dur.Seconds() < 1 {
		return nil
	}
	m.Log.Debugf("completed transfer of %s in %s (%s/s)", data.Size(total.Size), dur, data.Size(total.Size/int64(dur.Seconds())))
	return nil
}

func (m *Manager) updateStats(src, dst reflow.Repository, status transferStatus, stat stat) {
	// If there's no status object to report to, then don't bother with updating stats.
	if m.Status == nil {
		return
	}
	k := key(src) + key(dst)
	m.mu.Lock()
	t := m.tasks[k]
	if t == nil {
		t = &task{Task: m.Status.Startf("%s->%s", description(src), description(dst))}
		if m.tasks == nil {
			m.tasks = make(map[string]*task)
		}
		m.tasks[k] = t
	}
	t.Update(status, stat)
	t.Print(t)
	if t.Pending() {
		t.Done()
		delete(m.tasks, k)
	}
	m.managerStat.Update(status, stat)
	m.Status.Print(m.managerStat)
	m.mu.Unlock()
}

func (m *Manager) limiter(r reflow.Repository, lim *map[string]*limiter.Limiter, limits *Limits) *limiter.Limiter {
	m.mu.Lock()
	if *lim == nil {
		*lim = map[string]*limiter.Limiter{}
	}
	u := key(r)
	if (*lim)[u] == nil {
		(*lim)[u] = limiter.New()
		(*lim)[u].Release(limits.Limit(u))
	}
	l := (*lim)[u]
	m.mu.Unlock()
	return l
}

// Claim attempts to claim ownership of the transfer of the provided
// file from the given source to the given destination. Claim returns
// a fresh transfer and true when the claim is successful; it returns
// a current transfer and false when the transfer was not
// successfully claimed. This provides a mechanism for "single
// flighting" concurrent transfers.
func (m *Manager) claim(dst, src reflow.Repository, file reflow.File) (*transfer, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transfers == nil {
		m.transfers = make(map[transferKey]*transfer)
	}
	if m.files == nil {
		m.files = make(map[digest.Digest]reflow.File)
	}
	dig := file.Digest()
	key := transferKey{key(dst), dig}
	m.files[dig] = file
	if t := m.transfers[key]; t != nil {
		return t, false
	}
	t := &transfer{C: make(chan struct{})}
	m.transfers[key] = t
	return t, true
}

func (m *Manager) done(dst, src reflow.Repository, file reflow.File, err error) {
	m.mu.Lock()
	dig := file.Digest()
	key := transferKey{key(dst), dig}
	t := m.transfers[key]
	delete(m.transfers, key)
	delete(m.files, dig)
	m.mu.Unlock()
	t.Err = err
	close(t.C)
}

const maxDescription = 20

func description(r reflow.Repository) string {
	type shortStringer interface {
		ShortString() string
	}
	type stringer interface {
		String() string
	}
	var s string
	switch arg := r.(type) {
	case shortStringer:
		s = arg.ShortString()
	case stringer:
		s = arg.String()
	default:
		s = key(r)
	}
	if len(s) > maxDescription {
		s = s[:maxDescription-1] + ".."
	}
	return s
}

func key(r reflow.Repository) string {
	if url := r.URL(); url != nil {
		return url.String()
	}
	return fmt.Sprintf("anon:%p", r)
}
