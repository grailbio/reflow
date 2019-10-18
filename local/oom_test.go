package local

import (
	"context"
	"io/ioutil"
	"testing"
	"time"
)

const dockerFmt = "2006-01-02T15:04:05.999999999Z"

func isValidOOM(oomTime, start, end time.Time) bool {
	return oomTime.After(start) && oomTime.Before(end)
}

func TestLastOOMKill(t *testing.T) {
	testBootTime, err := time.Parse(dockerFmt, "2019-05-30T22:08:34.945074271Z")
	if err != nil {
		t.Fatal(err)
	}
	start, err := time.Parse(dockerFmt, "2019-05-30T22:09:34.945074271Z")
	if err != nil {
		t.Fatal(err)
	}
	// end is 11 minutes after start.
	end, err := time.Parse(dockerFmt, "2019-05-30T22:20:34.945074271Z")
	if err != nil {
		t.Fatal(err)
	}
	f, err := ioutil.TempFile("", "oomlogs")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`
1,1,1,-;abc
1,1,30000000,-;Out of memory: Kill process 1
1,1,1,-;defg
1,1,1,-;hijk
1,1,180000000,-;Out of memory: Kill process 3
1,1,1,-;lmnop
1,1,1231234124312,-;Out of memory: Kill process 123456
1,1,180000000,-;Memory cgroup out of memory: Kill process 4`)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	oomTracker := newOOMTracker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oomTracker.monitor(ctx, nil, f.Name(), testBootTime)
	for _, tt := range []struct {
		pid  int
		want bool
	}{
		{1, false}, // PID OOMKilled before start.
		{2, false}, // PID never OOMKilled.
		{3, true},  // PID OOMKilled after start but before end.
		{4, true},  // PID cgroup memory limit exceeded after start but before end.
	} {
		oomTime, ok := oomTracker.LastOOMKill(tt.pid)
		if got, want := isValidOOM(oomTime, start, end) && ok, tt.want; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
