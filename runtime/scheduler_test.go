package runtime

import (
	"fmt"
	"strings"
	"testing"

	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/test/infra"
)

func TestSchedulerDefaultPendingTransferLimit(t *testing.T) {
	config := infra.GetTestReflowConfig()
	var cluster runner.Cluster
	if err := config.Instance(&cluster); err != nil {
		t.Fatal(err)
	}
	scheduler, err := newScheduler(config, log.Std)
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
