package internal

import (
	"context"
	"fmt"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/trace"
)

type ExecState int

const (
	StateLoad ExecState = iota
	StatePut
	StateWait
	StateVerify
	StatePromote
	StateInspect
	StateResult
	StateTransferOut
	StateUnload
	StateDone
)

func (e ExecState) String() string {
	switch e {
	default:
		panic("bad state")
	case StateLoad:
		return "loading"
	case StatePut:
		return "submitting"
	case StateWait:
		return "waiting for completion"
	case StateVerify:
		return "verifying integrity"
	case StatePromote:
		return "promoting objects"
	case StateInspect:
		return "retrieving diagnostic output"
	case StateResult:
		return "retrieving result"
	case StateTransferOut:
		return "transferring output"
	case StateUnload:
		return "unloading"
	case StateDone:
		return "complete"
	}
}

func (e ExecState) TraceKind() trace.Kind {
	switch e {
	case StateLoad:
		return trace.Transfer
	case StatePromote:
		return trace.Transfer
	case StateTransferOut:
		return trace.Transfer
	default:
		return trace.Exec
	}
}

// Next returns the next state considering the `err` encountered after completing this state.
// It also returns a message (suitable for logging) explaining why the next state was chosen
// and whether the next state is a retry.
func (e ExecState) Next(ctx context.Context, err error, postUseChecksum bool) (next ExecState, nextIsRetry bool, msg string) {
	switch {
	case ctx.Err() != nil:
		msg = fmt.Sprintf("ctx.Err(): %v", ctx.Err())
		next = StateDone
	case err == nil:
		msg = "successful"
		next = e + 1
	case errors.NonRetryable(err):
		msg = fmt.Sprintf("non-retryable error: %v", err)
		next = StateDone
	default:
		msg = fmt.Sprintf("retryable error: %v", err)
		nextIsRetry = true
		// depending on the state, retrying might mean retrying the same step
		// or going back to a previous step
		next = e
		if e == StateWait {
			next = StatePut
		}
	}
	// Skip StateVerify unless post-use checksumming is enabled
	if next == StateVerify && !postUseChecksum {
		next++
	}
	return
}
