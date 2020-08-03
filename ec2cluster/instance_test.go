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
		r    reflow.Resources
		want string
	}{
		{reflow.Resources{"mem": 2 << 30, "cpu": 1, "disk": 10 << 30}, "t3a.medium"},
		{reflow.Resources{"mem": 10 << 30, "cpu": 5, "disk": 100 << 30}, "t3a.2xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 8, "disk": 800 << 30}, "r5.2xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 16, "disk": 800 << 30}, "m5a.4xlarge"},
		{reflow.Resources{"mem": 60 << 30, "cpu": 16, "disk": 400 << 30}, "r5.4xlarge"},
		{reflow.Resources{"mem": 122 << 30, "cpu": 16, "disk": 400 << 30}, "r5a.8xlarge"},
		{reflow.Resources{"mem": 60 << 30, "cpu": 32, "disk": 1000 << 30}, "c5.9xlarge"},
		{reflow.Resources{"mem": 120 << 30, "cpu": 32, "disk": 2000 << 30}, "r5a.8xlarge"},
	} {
		for _, spot := range []bool{true, false} {
			if got, _ := is.MinAvailable(tc.r, spot); got.Type != tc.want {
				t.Errorf("got %v, want %v for spot %v, resources %v", got.Type, tc.want, spot, tc.r)
			}
		}
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
