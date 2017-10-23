package local

import (
	"context"

	"github.com/grailbio/reflow"
)

// execState describes the current state of an exec.
type execState int

const (
	execUnstarted execState = iota // the exec state machine has yet to start
	execInit                       // the exec state machine has started
	execCreated                    // the Docker container has been created
	execRunning                    // the Docker container is running
	execComplete                   // the Docker container has completed running; the results are available
)

type exec interface {
	reflow.Exec
	Go(context.Context)
	WaitUntil(execState) error
	Kill(context.Context) error
}
