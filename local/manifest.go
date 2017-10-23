package local

import (
	"time"

	"github.com/docker/engine-api/types"
	"github.com/grailbio/reflow"
)

// execType defines the type of exec. It is used by the executor
// to (re-) construct the correct type of exec from disk.
type execType int

const (
	execDocker execType = iota
	execS3
)

// Manifest stores the state of an exec. It is serialized to JSON and
// stored on disk so that executors are restartable, and can recover
// from crashes.
type Manifest struct {
	Type  execType
	State execState

	Created time.Time

	Result    reflow.Result
	Config    reflow.ExecConfig   // The object config used to create this object.
	Docker    types.ContainerJSON // Docker inspect output.
	Resources reflow.Resources
	Stats     stats
	Gauges    reflow.Gauges
}
