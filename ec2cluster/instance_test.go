package ec2cluster

import (
	"testing"
	"time"

	"github.com/grailbio/reflow"
)

func TestInstanceState(t *testing.T) {
	var instances []instanceConfig
	for _, config := range instanceTypes {
		config.Resources["disk"] = float64(2000 << 30)
		instances = append(instances, config)
	}
	is := newInstanceState(instances, 1*time.Second, "us-west-2")
	for _, tc := range []struct {
		r                reflow.Resources
		wantMin, wantMax string
	}{
		{reflow.Resources{"mem": 2 << 30, "cpu": 1, "disk": 10 << 30}, "t3a.medium", "x1e.32xlarge"},
		{reflow.Resources{"mem": 10 << 30, "cpu": 5, "disk": 100 << 30}, "t3a.2xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 8, "disk": 800 << 30}, "r5.2xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 16, "disk": 800 << 30}, "m5a.4xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 60 << 30, "cpu": 16, "disk": 400 << 30}, "r5.4xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 122 << 30, "cpu": 16, "disk": 400 << 30}, "r5a.8xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 60 << 30, "cpu": 32, "disk": 1000 << 30}, "c5.9xlarge", "x1e.32xlarge"},
		{reflow.Resources{"mem": 120 << 30, "cpu": 32, "disk": 2000 << 30}, "r5a.8xlarge", "x1e.32xlarge"},
	} {
		for _, spot := range []bool{true, false} {
			if got, _ := is.MinAvailable(tc.r, spot); got.Type != tc.wantMin {
				t.Errorf("got %v, want %v for spot %v, resources %v", got.Type, tc.wantMin, spot, tc.r)
			}
			if got, _ := is.MaxAvailable(tc.r, spot); got.Type != tc.wantMax {
				t.Errorf("got %v, want %v for spot %v, resources %v", got.Type, tc.wantMax, spot, tc.r)
			}
		}
	}
}

func TestInstanceStateLargest(t *testing.T) {
	instances := newInstanceState(
		[]instanceConfig{instanceTypes["c5.2xlarge"]},
		1*time.Second, "us-west-2")
	if got, want := instances.Largest().Type, "c5.2xlarge"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	instances = newInstanceState(
		[]instanceConfig{instanceTypes["c5.2xlarge"], instanceTypes["c5.9xlarge"]},
		1*time.Second, "us-west-2")
	if got, want := instances.Largest().Type, "c5.9xlarge"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	instances = newInstanceState(
		[]instanceConfig{instanceTypes["r5a.8xlarge"], instanceTypes["c5.9xlarge"]},
		1*time.Second, "us-west-2")
	if got, want := instances.Largest().Type, "r5a.8xlarge"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestConfigureEBS(t *testing.T) {
	type ebsInfo struct {
		ebsType string
		ebsSize uint64
		nebs    int
	}
	for _, tc := range []struct {
		e, want ebsInfo
	}{
		{ebsInfo{"st1", 0, 0}, ebsInfo{"st1", 500, 1}},
		{ebsInfo{"st1", 100, 2}, ebsInfo{"st1", 500, 1}},
		{ebsInfo{"st1", 501, 2}, ebsInfo{"st1", 1000, 2}},
		{ebsInfo{"st1", 1300, 2}, ebsInfo{"st1", 1300, 2}},
		{ebsInfo{"gp2", 0, 0}, ebsInfo{"gp2", 1, 1}},
		{ebsInfo{"gp2", 10, 5}, ebsInfo{"gp2", 10, 5}},
		{ebsInfo{"gp2", 500, 4}, ebsInfo{"gp2", 500, 4}},
		{ebsInfo{"gp2", 1200, 5}, ebsInfo{"gp2", 1336, 4}},
		{ebsInfo{"gp2", 2200, 10}, ebsInfo{"gp2", 2338, 7}},
	} {
		i := &instance{EBSType: tc.e.ebsType, EBSSize: tc.e.ebsSize, NEBS: tc.e.nebs}
		i.configureEBS()
		got := ebsInfo{i.EBSType, i.EBSSize, i.NEBS}
		if got != tc.want {
			t.Errorf("given %v: got %v, want %v", tc.e, got, tc.want)
		}
	}
}
