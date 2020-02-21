package sched

import (
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/taskdb"
)

func TestGetTaskGroups(t *testing.T) {
	var (
		img      = "image"
		cmd      = "cmd"
		ident    = "ident"
		imgCmdID = taskdb.NewImgCmdID(img, cmd)
		task     = &Task{
			Config: reflow.ExecConfig{
				Image: img,
				Cmd:   cmd,
				Ident: ident,
			},
		}
	)

	taskGroups := getTaskGroups(task)
	if got, want := len(taskGroups), 2; got != want {
		t.Fatalf("got %d taskgroups, want %d", got, want)
	}

	imgcmdgroup := taskGroups[0]
	if _, ok := imgcmdgroup.(imgCmdGroup); !ok {
		t.Fatalf("imgcmd group type: got %T, want %T", imgcmdgroup, imgCmdGroup{})
	}
	if got, want := imgcmdgroup.Name(), "imgCmdGroup:"+imgCmdID.ID(); got != want {
		t.Errorf("imgcmd group name: got %s, want %s", got, want)
	}
	imgcmdQuery := imgcmdgroup.Query()
	if got, want := imgcmdQuery.ImgCmdID.ID(), imgCmdID.ID(); got != want {
		t.Errorf("imgcmd group query imgcmdid: got %s, want %s", got, want)
	}

	identgroup := taskGroups[1]
	if _, ok := identgroup.(identGroup); !ok {
		t.Fatalf("ident group type: got %T, want %T", identgroup, identGroup{})
	}
	if got, want := identgroup.Name(), "identGroup:"+ident; got != want {
		t.Errorf("ident group name: got %s, want %s", got, want)
	}
	identQuery := identgroup.Query()
	if got, want := identQuery.Ident, ident; got != want {
		t.Errorf("ident group query ident: got %s, want %s", got, want)
	}
}
