package tool

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/test/infra"
	wg2 "github.com/grailbio/reflow/wg"
)

func TestSchedulerDefaultPendingTransferLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := infra.GetTestReflowConfig()
	scheduler, err := NewScheduler(ctx, config, &wg2.WaitGroup{}, nil, log.Std, new(status.Status))
	if err != nil {
		t.Fatal(err)
	}
	// This is fragile. Due to lack of better ideas, for now...
	// TODO(pgopal): may be expose this as an API?
	manager, ok := scheduler.Transferer.(*repository.Manager)
	if !ok {
		t.Fatal("scheduler transferer not repository.Manager")
	}
	expectedLimit := fmt.Sprintf("limits{def:%d overrides:", defaultTransferLimit)
	if !strings.HasPrefix(manager.PendingTransfers.String(), expectedLimit) {
		t.Fatalf("expected prefix %s, got %s", expectedLimit, manager.PendingTransfers.String())
	}
}
