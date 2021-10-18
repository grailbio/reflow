package infra

import (
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/infra"
)

func TestReflowletConfig(t *testing.T) {
	var schema = infra.Schema{
		"reflowlet": new(ReflowletConfig),
	}
	for _, tt := range []struct {
		keyVal string
		want ReflowletConfig
	}{
		{
			"reflowletconfig",
			DefaultReflowletConfig,
		},
		{
			"reflowletconfig,maxidleduration=1m",
			MergeReflowletConfig(DefaultReflowletConfig, ReflowletConfig{
				MaxIdleDuration: time.Minute}),
		},
		{
			"reflowletconfig,lowthresholdpct=25.0",
			MergeReflowletConfig(DefaultReflowletConfig, ReflowletConfig{
				VolumeWatcher: VolumeWatcher{LowThresholdPct: 25.0}}),
		},
		{
			"reflowletconfig,lowthresholdpct=25.0,resizesleepduration=2s",
			MergeReflowletConfig(DefaultReflowletConfig, ReflowletConfig{
				VolumeWatcher: VolumeWatcher{LowThresholdPct: 25.0, ResizeSleepDuration: 2 * time.Second}}),
		},
	}{
		config, err := schema.Make(infra.Keys{"reflowlet": tt.keyVal})
		if err != nil {
			t.Fatal(err)
		}
		var rc *ReflowletConfig
		if err = config.Instance(&rc); err != nil {
			t.Fatal(err)
		}
		if got, want := *rc, tt.want; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
