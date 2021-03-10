package internal

import (
	"context"
	"testing"

	"github.com/grailbio/reflow/errors"
)

func TestExecStateNext(t *testing.T) {
	bgCtx := context.Background()
	cancledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name            string
		state           ExecState
		ctx             context.Context
		err             error
		postUseChecksum bool
		wantNext        ExecState
		wantNextIsRetry bool
	}{
		{
			name:            "ctx error should go to stateDone",
			state:           StateWait,
			ctx:             cancledCtx,
			err:             nil,
			postUseChecksum: false,
			wantNext:        StateDone,
			wantNextIsRetry: false,
		},
		{
			name:            "nil error should go to next sequential state",
			state:           StateLoad,
			ctx:             bgCtx,
			err:             nil,
			postUseChecksum: false,
			wantNext:        StatePut,
			wantNextIsRetry: false,
		},
		{
			name:            "non-retryable error should go to stateDone",
			state:           StateWait,
			ctx:             bgCtx,
			err:             errors.E(errors.Fatal, "some fatal error"),
			postUseChecksum: false,
			wantNext:        StateDone,
			wantNextIsRetry: false,
		},
		{
			name:            "retryable stateWait error should go to statePut",
			state:           StateWait,
			ctx:             bgCtx,
			err:             errors.E(errors.Fatal, "some fatal error"),
			postUseChecksum: false,
			wantNext:        StateDone,
			wantNextIsRetry: false,
		},
		{
			name:            "retryable stateWait error should go to statePut",
			state:           StateWait,
			ctx:             bgCtx,
			err:             errors.E(errors.Unavailable, "some transient error"),
			postUseChecksum: false,
			wantNext:        StatePut,
			wantNextIsRetry: true,
		},
		{
			name:            "retryable errors (other than in stateWait) should retry same state",
			state:           StateUnload,
			ctx:             bgCtx,
			err:             errors.E(errors.Other, "some other error"),
			postUseChecksum: false,
			wantNext:        StateUnload,
			wantNextIsRetry: true,
		},
		{
			name:            "successful stateWait with postUseChecksum should go to stateVerify",
			state:           StateWait,
			ctx:             bgCtx,
			err:             nil,
			postUseChecksum: true,
			wantNext:        StateVerify,
			wantNextIsRetry: false,
		},
		{
			name:            "successful stateWait without postUseChecksum should go to statePromote",
			state:           StateWait,
			ctx:             bgCtx,
			err:             nil,
			postUseChecksum: false,
			wantNext:        StatePromote,
			wantNextIsRetry: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotNext, gotNextIsRetry, _ := tc.state.Next(tc.ctx, tc.err, tc.postUseChecksum)
			if gotNext != tc.wantNext {
				t.Errorf("got next: %d, want: %d", gotNext, tc.wantNext)
			}
			if gotNextIsRetry != tc.wantNextIsRetry {
				t.Errorf("got isRetry: %v, want: %v", gotNextIsRetry, tc.wantNextIsRetry)
			}
		})
	}
}
