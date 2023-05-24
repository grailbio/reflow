package local

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/repository/filerepo"
)

type downloadKey struct {
	bucket blob.Bucket
	key    string
}

type downloadManager struct {
	downloadingFiles map[downloadKey]*downloadTask
	pastDownloads    map[digest.Digest]downloadKey
	permanentRepo    *filerepo.Repository
	newDownloadMu    sync.Mutex
	completions      chan completedDownload
}

type completedDownload struct {
	bucket blob.Bucket
	prefix string
	file   reflow.File
	key    digest.Digest
	err    error
}

type downloadObserver struct {
	observer chan completedDownload
	key      digest.Digest
}

func newDownloadManagerWithRepository(repo *filerepo.Repository) *downloadManager {
	dm := &downloadManager{
		downloadingFiles: make(map[downloadKey]*downloadTask, 0),
		pastDownloads:    make(map[digest.Digest]downloadKey, 0),
		permanentRepo:    repo,
		completions:      make(chan completedDownload),
	}
	go dm.watchForUpdates()
	return dm
}

func (dm *downloadManager) watchForUpdates() {
	for {
		select {
		case c := <-dm.completions:
			if c.err == nil {
				d := c.file.Digest()
				dm.newDownloadMu.Lock()
				dm.pastDownloads[d] = downloadKey{
					bucket: c.bucket,
					key:    c.prefix,
				}
				dm.newDownloadMu.Unlock()
			}
		}
	}
}

func (dm *downloadManager) removeDownload(id digest.Digest) {
	pd := dm.pastDownloads[id]
	delete(dm.downloadingFiles, downloadKey{bucket: pd.bucket, key: pd.key})
	delete(dm.pastDownloads, id)
}

func (dm *downloadManager) Acquire(ctx context.Context, dl *download, observer chan completedDownload, key digest.Digest) {
	dm.newDownloadMu.Lock()
	var (
		dt *downloadTask
		ok bool
	)
	do := downloadObserver{observer: observer, key: key}
	dlKey := downloadKey{
		bucket: dl.Bucket,
		key:    dl.Key,
	}
	if dt, ok = dm.downloadingFiles[dlKey]; !ok {
		dt = &downloadTask{
			dl:        dl,
			observers: []downloadObserver{{observer: dm.completions}, do},
		}
		dm.downloadingFiles[dlKey] = dt
		go dt.Start(ctx, dm.permanentRepo)
		dt.dl.Log.Printf("Started new download task for %s%s", dl.Bucket.Location(), dl.Key)
	} else {
		dt.addObserver(do)
	}
	dm.newDownloadMu.Unlock()
}

type downloadTask struct {
	dl            *download
	observers     []downloadObserver
	observerAddMu sync.Mutex
	product       *reflow.File
	err           error
}

func (dt *downloadTask) Start(ctx context.Context, permRepo *filerepo.Repository) {
	var (
		tempRepo filerepo.Repository
		err      error
		res      reflow.File
	)
	tempRepo.Root, err = ioutil.TempDir(permRepo.Root, "temp-load")
	if err != nil {
		dt.dl.Log.Errorf("error creating temp-repo-dir: %v", err)
		return
	}
	defer os.RemoveAll(tempRepo.Root)
	res, err = dt.dl.Do(ctx, &tempRepo)
	if err != nil {
		dt.err = err
		dt.alertObservers(completedDownload{err: err})
		return
	}
	if err := permRepo.Vacuum(ctx, &tempRepo); err != nil {
		dErr := fmt.Errorf("error copying file into repo: %v", err)
		dt.err = dErr
		dt.alertObservers(completedDownload{err: dErr})
		return
	}
	dt.product = &res // Future observers will be sent the file directly
	dt.alertObservers(completedDownload{file: res, bucket: dt.dl.Bucket, prefix: dt.dl.Key})
}

func (dt *downloadTask) alertObservers(c completedDownload) {
	dt.observerAddMu.Lock()
	dt.dl.Log.Printf("Notifying %d observers for %s%s", len(dt.observers), dt.dl.Bucket.Location(), dt.dl.Key)
	for _, ch := range dt.observers {
		c.key = ch.key
		ch.observer <- c
	}
	dt.observerAddMu.Unlock()
}

func (dt *downloadTask) addObserver(observer downloadObserver) {
	dt.observerAddMu.Lock()
	if dt.err != nil {
		go func(o chan completedDownload) {
			o <- completedDownload{err: dt.err}
		}(observer.observer)
	} else if dt.product != nil {
		go func(o chan completedDownload) {
			o <- completedDownload{file: *dt.product, key: observer.key}
		}(observer.observer)
	} else {
		dt.observers = append(dt.observers, observer)
	}
	dt.observerAddMu.Unlock()
}
