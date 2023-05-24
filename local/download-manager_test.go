package local

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/repository/filerepo"
)

func TestDownloadManagerAcquireCreatesOneTaskPerDownloadWhenDownloadTakesSignificantTime(t *testing.T) {
	downloads := make([]download, 0)
	bucket := MockBucket{location: "local-file", duration: 1 * time.Second}
	for i := 0; i < 3; i++ {
		downloads = append(downloads, download{
			Bucket: bucket,
			Key:    "sample/key",
		})
	}
	repoRoot, _ := ioutil.TempDir("/tmp", "file-repo-")
	repo := filerepo.Repository{Root: repoRoot}
	dm := newDownloadManagerWithRepository(&repo)
	ctx := context.TODO()
	for _, download := range downloads {
		dm.Acquire(ctx, &download, make(chan completedDownload), digest.Digest{})
	}
	if len(dm.downloadingFiles) != 1 {
		t.Errorf("created %d downloads for many files with the same name: %v", len(dm.downloadingFiles), dm.downloadingFiles)
	}
}

func TestDownloadManagerAlertsAllObserversWhenDone(t *testing.T) {
	downloads := make([]download, 0)
	observers := make([]chan completedDownload, 0)
	bucket := MockBucket{location: "local-file", duration: 1 * time.Second}
	for i := 0; i < 3; i++ {
		downloads = append(downloads, download{
			Bucket: bucket,
			Key:    "sample/key",
		})
		observers = append(observers, make(chan completedDownload))
	}
	repoRoot, _ := ioutil.TempDir("/tmp", "file-repo-")
	repo := filerepo.Repository{Root: repoRoot}
	dm := newDownloadManagerWithRepository(&repo)
	ctx := context.TODO()
	for idx, download := range downloads {
		dm.Acquire(ctx, &download, observers[idx], digest.Digest{})
	}
	for i := 0; i < len(observers); i++ {
		select {
		case <-observers[i]:
		case <-time.After(1 * time.Minute):
			t.Fatalf("Stopped receiving files on observe %d", i)
		}
	}
}

func TestDownloadManagerReturnsFileWhenFileWasPreviouslyDownloaded(t *testing.T) {
	downloads := make([]download, 0)
	observers := make([]chan completedDownload, 0)
	runDuration := 1 * time.Second
	bucket := MockBucket{location: "local-file", duration: runDuration}
	for i := 0; i < 2; i++ {
		downloads = append(downloads, download{
			Bucket: bucket,
			Key:    "sample/key",
			File:   reflow.File{},
			Log:    nil,
		})
		observers = append(observers, make(chan completedDownload))
	}
	repoRoot, _ := ioutil.TempDir("/tmp", "file-repo-")
	repo := filerepo.Repository{Root: repoRoot}
	dm := newDownloadManagerWithRepository(&repo)
	ctx := context.TODO()
	for idx, download := range downloads {
		dm.Acquire(ctx, &download, observers[idx], digest.Digest{})
	}
	for _, obs := range observers {
		<-obs
	}

	newObserver := make(chan completedDownload)
	finished := make(chan struct{})
	go func(newObserver chan completedDownload) {
		select {
		case <-newObserver:
			close(finished)
		}
	}(newObserver)
	dm.Acquire(ctx, &download{
		Bucket: bucket,
		Key:    "sample/key",
	}, newObserver, digest.Digest{})

	select {
	case <-time.After(6 * runDuration):
		t.Fatalf("Could not acquire file before timeout")
	case <-finished:

	}
}

func TestDownloadManagerReturnsErrorToObserversWhenErrorOccursInInitialDownload(t *testing.T) {
	downloads := make([]download, 0)
	observers := make([]chan completedDownload, 0)
	bucket := MockBucket{location: "local-file", duration: 1 * time.Second, shouldError: true}
	for i := 0; i < 3; i++ {
		downloads = append(downloads, download{
			Bucket: bucket,
			Key:    "sample/key",
		})
		observers = append(observers, make(chan completedDownload))
	}
	repoRoot, _ := ioutil.TempDir("/tmp", "file-repo-")
	repo := filerepo.Repository{Root: repoRoot}
	dm := newDownloadManagerWithRepository(&repo)
	ctx := context.TODO()
	for idx, download := range downloads {
		dm.Acquire(ctx, &download, observers[idx], digest.Digest{})
	}
	for i := 0; i < len(observers); i++ {
		select {
		case res := <-observers[i]:
			if res.err == nil {
				t.Fatalf("Should have receievd error")
			}
		case <-time.After(1 * time.Minute):
			t.Fatalf("Stopped receiving files on observe %d", i)
		}
	}
}

func TestDownloadManagerReturnsErrorsToObserversAddedLaterWhenInitialDownloadOccurs(t *testing.T) {
	downloads := make([]download, 0)
	observers := make([]chan completedDownload, 0)
	runDuration := 1 * time.Second
	bucket := MockBucket{location: "local-file", duration: runDuration, shouldError: true}
	for i := 0; i < 2; i++ {
		downloads = append(downloads, download{
			Bucket: bucket,
			Key:    "sample/key",
			File:   reflow.File{},
			Log:    nil,
		})
		observers = append(observers, make(chan completedDownload))
	}
	repoRoot, _ := ioutil.TempDir("/tmp", "file-repo-")
	repo := filerepo.Repository{Root: repoRoot}
	dm := newDownloadManagerWithRepository(&repo)
	ctx := context.TODO()
	for idx, download := range downloads {
		dm.Acquire(ctx, &download, observers[idx], digest.Digest{})
	}
	for _, obs := range observers {
		<-obs
	}

	newObserver := make(chan completedDownload)
	finished := make(chan struct{})
	go func(newObserver chan completedDownload) {
		select {
		case res := <-newObserver:
			if res.err == nil {
				t.Fatalf("should have received error")
			}
			close(finished)
		}
	}(newObserver)
	dm.Acquire(ctx, &download{
		Bucket: bucket,
		Key:    "sample/key",
	}, newObserver, digest.Digest{})

	select {
	case <-time.After(6 * runDuration):
		t.Fatalf("Could not acquire file before timeout")
	case <-finished:

	}
}

type MockBucket struct {
	location    string
	duration    time.Duration
	shouldError bool
}

func (m MockBucket) Download(ctx context.Context, key, etag string, size int64, w io.WriterAt) (int64, error) {
	time.Sleep(m.duration)
	if m.shouldError {
		return 0, fmt.Errorf("Download error")
	}
	count, err := w.WriteAt([]byte{0x0, 0x1}, 0)
	return int64(count), err
}

func (m MockBucket) Location() string {
	return m.location
}

func (m MockBucket) File(ctx context.Context, key string) (reflow.File, error) {
	panic("implement me")
}

func (m MockBucket) Scan(prefix string, withMetadata bool) blob.Scanner {
	panic("implement me")
}

func (m MockBucket) Get(ctx context.Context, key, etag string) (io.ReadCloser, reflow.File, error) {
	panic("implement me")
}

func (m MockBucket) Put(ctx context.Context, key string, size int64, body io.Reader, contentHash string) error {
	panic("implement me")
}

func (m MockBucket) Snapshot(ctx context.Context, prefix string) (reflow.Fileset, error) {
	panic("implement me")
}

func (m MockBucket) Copy(ctx context.Context, src, dst, contentHash string) error {
	panic("implement me")
}

func (m MockBucket) CopyFrom(ctx context.Context, srcBucket blob.Bucket, src, dst string) error {
	panic("implement me")
}

func (m MockBucket) Delete(ctx context.Context, keys ...string) error {
	panic("implement me")
}
