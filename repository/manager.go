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
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"golang.org/x/sync/errgroup"
)

const (
	// minBPS defines the lowest acceptable transfer rate.
	minBPS = 1 << 20
	// minTimeout defines the smallest acceptable timeout.
	// This helps to give wiggle room for small data transfers.
	minTimeout = 30 * time.Second

	// The minimum file size for the purposes of reservation.
	// This is used to limit file transfer concurrency.
	minSize = 500 << 20
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

// stat is used to keep track of pending transfer statistics.
type stat struct {
	files, send, recv int
}

// A Manager is used to transfer objects between repositories while
// enforcing transfer policies.
//
// BUG(marius): Manager does not release references to repositories;
// in long-term processes, this could cause space leaks.
type Manager struct {
	// Log is used to report manager status.
	Log *log.Logger

	// PendingBytes defines limits for the number of outstanding
	// bytes a repository (keyed by URL) may have in transfer.
	// The limit is used separately for transmit and receive traffic.
	// It is not possible currently to set different limits for the two
	// directions.
	PendingBytes *Limits

	// Stat defines limits for the number of stat operations that
	// may be issued concurrently to any given repository.
	Stat *Limits

	mu             sync.Mutex
	src, dst, stat map[string]*limiter.Limiter
	pending        map[string]stat
}

// Report reports transfer status to m.Logger at each interval.
func (m *Manager) Report(ctx context.Context, interval time.Duration) {
	if !m.Log.At(log.DebugLevel) {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// We accumulate these first so we don't print under lock.
			// (Loggers may block.)
			var entries []string
			m.mu.Lock()
			for u, stat := range m.pending {
				lim := m.PendingBytes.Limit(u)
				entries = append(entries, fmt.Sprintf(
					"%s: files:%d send:%s/%s recv:%s/%s", u, stat.files,
					data.Size(stat.send), data.Size(lim),
					data.Size(stat.recv), data.Size(lim)))
			}
			m.mu.Unlock()
			for _, line := range entries {
				m.Log.Debug(line)
			}
		}
	}
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
	return m.transfer(ctx, dst, src, files...)
}

// NeedTransfer determines which of the provided files are missing from
// the destination repository and must therefore be transfered.
func (m *Manager) NeedTransfer(ctx context.Context, dst reflow.Repository, files ...reflow.File) ([]reflow.File, error) {
	exists := make([]bool, len(files))
	lstat := m.limiter(dst, &m.stat, m.Stat)
	g, gctx := errgroup.WithContext(ctx)
	for i, file := range files {
		if err := lstat.Acquire(gctx, 1); err != nil {
			return nil, err
		}
		i, file := i, file
		// TODO(marius): implement a batch stat call.
		// It will be more efficient in most cases.
		g.Go(func() error {
			ctx, cancel := context.WithTimeout(gctx, 10*time.Second)
			_, err := dst.Stat(ctx, file.ID)
			lstat.Release(1)
			cancel()
			exists[i] = err == nil
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
		lx = m.limiter(dst, &m.dst, m.PendingBytes)
		ly = m.limiter(src, &m.src, m.PendingBytes)
		ux = key(dst)
		uy = key(src)
	)
	if uy < ux {
		ux, uy = uy, ux
		lx, ly = ly, lx
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := range files {
		file := files[i]
		n := int(file.Size)
		nx, ny := n, n
		if nx < minSize {
			nx = minSize
		}
		if lim := m.PendingBytes.Limit(ux); lim < nx {
			nx = lim
		}
		if err := lx.Acquire(ctx, nx); err != nil {
			return err
		}
		if ny < minSize {
			ny = minSize
		}
		if lim := m.PendingBytes.Limit(uy); lim < ny {
			ny = lim
		}
		if err := ly.Acquire(ctx, ny); err != nil {
			lx.Release(nx)
			return err
		}
		g.Go(func() error {
			m.updateStats(dst, 1, 0, n)
			m.updateStats(src, 1, n, 0)
			// Note: this is too coarse grained. It means that
			// for large objects, we end up waiting a long time
			// to detect stalled transfers. Instead, we should enforce
			// transfer progress, and also introduce failure detectors
			// between peer nodes.
			timeout := time.Duration(file.Size/minBPS) * time.Second
			if timeout < minTimeout {
				timeout = minTimeout
			}
			ctx, cancel := context.WithTimeout(ctx, timeout)
			err := Transfer(ctx, dst, src, file.ID)
			cancel()
			ly.Release(ny)
			lx.Release(nx)
			m.updateStats(dst, -1, 0, -n)
			m.updateStats(src, -1, -n, 0)
			return err
		})
	}
	return g.Wait()
}

func (m *Manager) updateStats(r reflow.Repository, files, send, recv int) {
	u := key(r)
	m.mu.Lock()
	s := m.pending[u]
	s.files += files
	s.send += send
	s.recv += recv
	if m.pending == nil {
		m.pending = map[string]stat{}
	}
	m.pending[u] = s
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

func key(r reflow.Repository) string {
	if url := r.URL(); url != nil {
		return url.String()
	}
	return fmt.Sprintf("anon:%p", r)
}
