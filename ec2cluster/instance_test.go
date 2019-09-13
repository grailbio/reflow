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
		{reflow.Resources{"mem": 2 << 30, "cpu": 1, "disk": 10 << 30}, "c5.large"},
		{reflow.Resources{"mem": 10 << 30, "cpu": 5, "disk": 100 << 30}, "c5.2xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 8, "disk": 800 << 30}, "r5.2xlarge"},
		{reflow.Resources{"mem": 30 << 30, "cpu": 16, "disk": 800 << 30}, "m5.4xlarge"},
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
