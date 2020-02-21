package sched

import "github.com/grailbio/reflow/taskdb"

// taskGroup defines a group of tasks.
type taskGroup interface {
	// Name returns the taskGroup's name.
	Name() string
	// Query returns a TaskQuery that produces all of the tasks in the taskGroup from TaskDB.
	Query() taskdb.TaskQuery
}

// imgCmdGroup is a taskGroup that groups tasks by their
// behavior. A task's behavior is described by its underlying
// exec's docker image + cmd.
type imgCmdGroup struct {
	imgCmdID taskdb.ImgCmdID
}

// Name returns the imgCmdGroup's name.
func (i imgCmdGroup) Name() string {
	return "imgCmdGroup:" + i.imgCmdID.ID()
}

// Query returns a TaskQuery that produces all of the tasks in the imgCmdGroup from TaskDB.
func (i imgCmdGroup) Query() taskdb.TaskQuery {
	return taskdb.TaskQuery{
		ImgCmdID: i.imgCmdID,
	}
}

// identGroup is a taskGroup that groups tasks by their
// underlying execs' human-readable identifier. This
// identifier is the name of the reflow function that
// calls the task's exec.
type identGroup struct {
	ident string
}

// Name returns the identGroup's name.
func (i identGroup) Name() string {
	return "identGroup:" + i.ident
}

// Query returns a TaskQuery that produces all of the tasks in the identGroup from TaskDB.
func (i identGroup) Query() taskdb.TaskQuery {
	return taskdb.TaskQuery{
		Ident: i.ident,
	}
}

// getTaskGroups returns a slice of taskGroups in order of
// decreasing specificity (imgCmdGroup > identGroup).
func getTaskGroups(task *Task) []taskGroup {
	return []taskGroup{
		imgCmdGroup{imgCmdID: taskdb.NewImgCmdID(task.Config.Image, task.Config.Cmd)},
		identGroup{ident: task.Config.Ident},
	}
}
