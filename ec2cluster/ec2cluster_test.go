// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/data"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws/test"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/runner"
)

func TestGetEC2State(t *testing.T) {
	var ec2Is []*ec2.Instance
	for _, state := range []string{"terminated", "stopped", "stopping", "shutting-down"} {
		i, _ := create("i-"+state, state, "", "")
		ec2Is = append(ec2Is, i)
	}
	i1, ri1 := create("i-run", "running", "", "")
	ec2Is = append(ec2Is, i1)
	i2, ri2 := create("i-run2", "running", "v1", "dig1")
	ec2Is = append(ec2Is, i2)
	dio := &ec2.DescribeInstancesOutput{Reservations: []*ec2.Reservation{{Instances: ec2Is}}}
	client := mockEC2Client{output: dio}
	c := &Cluster{EC2: &client}
	instances, _ := c.getEC2State(context.Background())
	if got, want := len(instances), 2; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, want := instances["i-run"], ri1; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := instances["i-run2"], ri2; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestRefresh(t *testing.T) {
	var ec2Is []*ec2.Instance
	for _, state := range []string{"terminated", "shutting-down", "running"} {
		i, _ := create("i-"+state, state, "", "")
		ec2Is = append(ec2Is, i)
	}
	dio := &ec2.DescribeInstancesOutput{Reservations: []*ec2.Reservation{{Instances: ec2Is}}}
	mockEC2 := mockEC2Client{output: dio}
	c := &Cluster{EC2: &mockEC2, stats: newStats(), pools: make(map[string]reflowletPool)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := c.Refresh(ctx); err != nil {
		t.Errorf("reconcile: %v", err)
	}
	checkState(t, c, "i-running")

	// Alter EC2 state (add instance) and reconcile
	i, _ := create("i-another", "running", "", "")
	ec2Is = append(ec2Is, i)
	dio.Reservations[0].Instances = ec2Is
	mockEC2.output = dio
	if _, err := c.Refresh(ctx); err != nil {
		t.Errorf("reconcile: %v", err)
	}
	// Verify
	checkState(t, c, "i-running", "i-another")

	// Alter EC2 state (terminate instance) and reconcile
	for _, inst := range dio.Reservations[0].Instances {
		if *inst.InstanceId == "i-another" {
			inst.State = &ec2.InstanceState{Name: aws.String("terminated")}
		}
	}
	mockEC2.output = dio
	if _, err := c.Refresh(ctx); err != nil {
		t.Errorf("reconcile: %v", err)
	}
	// Verify
	checkState(t, c, "i-running")
	cancel()
}

func create(id, state, version, digest string) (*ec2.Instance, *reflowletInstance) {
	inst := &ec2.Instance{
		InstanceId:    aws.String(id),
		InstanceType:  aws.String("type-" + id),
		PublicDnsName: aws.String(id + ".test.grail.com"),
		State:         &ec2.InstanceState{Name: aws.String(state)},
	}
	if version != "" {
		inst.Tags = append(inst.Tags, &ec2.Tag{Key: aws.String("reflowlet:version"), Value: aws.String(version)})
	}
	if digest != "" {
		inst.Tags = append(inst.Tags, &ec2.Tag{Key: aws.String("reflowlet:digest"), Value: aws.String(digest)})
	}
	return inst, &reflowletInstance{*inst, version, digest}
}

func checkState(t *testing.T, c *Cluster, instanceIds ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	var gots []string
	for g := range c.pools {
		gots = append(gots, g)
	}
	setEquals(t, "pool", gots, instanceIds)
}

func setEquals(t *testing.T, msg string, gots, wants []string) {
	t.Helper()
	if got, want := len(gots), len(wants); got != want {
		t.Fatalf("%s size: got %d, want %d", msg, got, want)
	}
	wantsMap := make(map[string]bool, len(wants))
	for _, w := range wants {
		wantsMap[w] = true
	}
	gotsMap := make(map[string]bool, len(gots))
	for _, w := range gots {
		gotsMap[w] = true
	}
	for _, w := range wants {
		if _, ok := gotsMap[w]; !ok {
			t.Fatalf("%s: want %s missing in [%s]", msg, w, strings.Join(gots, ","))
		}
	}
	for _, g := range gots {
		if _, ok := wantsMap[g]; !ok {
			t.Fatalf("%s: got %s absent in [%s]", msg, g, strings.Join(wants, ","))
		}
	}
}

func TestClusterInfra(t *testing.T) {
	const bootstrapImage = "https://some_s3_path"
	validateBootstrap = func(burl string, _ header) error {
		if burl != bootstrapImage {
			return fmt.Errorf("got %v, want %v", burl, bootstrapImage)
		}
		return nil
	}
	var schema = getInfraSchema()
	for _, tt := range []struct {
		b        string
		name, rv string
		spot     bool
	}{
		{`
        labels: kv
        tls: tls,file=/tmp/ca
        logger: logger
        session: fakesession
        user: user
        bootstrap: bootstrapimage,uri=` + bootstrapImage + `
        reflow: reflowversion,version=abcdef
        cluster: ec2cluster
        ec2cluster:
            maxinstances: 1
            disktype: dt
            diskspace: 10
            ami: foo
            region: bar
            securitygroup: blah
        sshkey: key
        `,
			"default", "abcdef", false},
	} {
		config, err := schema.Unmarshal([]byte(tt.b))
		if err != nil {
			t.Fatal(err)
		}
		var cluster runner.Cluster
		config.Must(&cluster)
		ec2cluster, ok := cluster.(*Cluster)
		if !ok {
			t.Fatalf("%v is not an ec2cluster", reflect.TypeOf(cluster))
		}
		if got, want := ec2cluster.Name, tt.name; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := ec2cluster.Spot, tt.spot; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := ec2cluster.ReflowVersion, tt.rv; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestValidateBootstrap(t *testing.T) {
	for _, tc := range []struct {
		burl      string
		h         header
		wantError bool
	}{
		{"", nil, true},
		{"619867110810.dkr.ecr.us-west-2.amazonaws.com/reflowbootstrap:reflowbootstrap", nil, true},
		{"http://path_to_bootstrap", nil, true},
		{"https://path_to_bootstrap", &mockHeader{err: fmt.Errorf("test error")}, true},
		{"https://path_to_bootstrap", &mockHeader{resp: &http.Response{StatusCode: http.StatusForbidden}}, true},
		{"https://path_to_bootstrap", &mockHeader{resp: &http.Response{StatusCode: http.StatusOK, Header: map[string][]string{"Content-Type": {"text/plain"}}}}, true},
		{"https://path_to_bootstrap", &mockHeader{resp: &http.Response{StatusCode: http.StatusOK, Header: map[string][]string{"Content-Type": {"binary/octet-stream"}}}}, false},
	} {
		got := defaultValidateBootstrap(tc.burl, tc.h)
		if tc.wantError != (got != nil) {
			t.Errorf("validateBootstrap(%s): got: %v want: %v", tc.burl, got, tc.wantError)
		}
	}
}

type mockHeader struct {
	resp *http.Response
	err  error
}

func (e *mockHeader) Head(url string) (resp *http.Response, err error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.resp, nil
}

func getInfraSchema() infra.Schema {
	return infra.Schema{
		"labels":    make(pool.Labels),
		"cluster":   new(runner.Cluster),
		"tls":       new(tls.Certs),
		"logger":    new(log.Logger),
		"session":   new(session.Session),
		"user":      new(infra2.User),
		"bootstrap": new(infra2.BootstrapImage),
		"reflow":    new(infra2.ReflowVersion),
		"sshkey":    new(infra2.SshKey),
	}
}

func getEC2ClusterWithRestrictedInstanceTypes() (*Cluster, error) {
	var (
		config infra.Config
		err    error
	)
	var schema = getInfraSchema()
	configYaml := `
        labels: kv
        tls: tls,file=/tmp/ca
        logger: logger
        session: fakesession
        user: user
        bootstrap: bootstrapimage,uri=https://some_s3_path
        reflow: reflowversion,version=abcdef
        cluster: ec2cluster
        ec2cluster:
            maxinstances: 1
            disktype: dt
            diskspace: 10
            ami: foo
            region: us-west-2
            securitygroup: blah
            instancetypes:
            - c5.large
            - c5.4xlarge
            - r3.8xlarge
        sshkey: key
    `
	if config, err = schema.Unmarshal([]byte(configYaml)); err != nil {
		return nil, err
	}
	var cluster runner.Cluster
	if err := config.Instance(&cluster); err != nil {
		return nil, err
	}
	ec2cluster, ok := cluster.(*Cluster)
	if !ok {
		return nil, fmt.Errorf("%v is not an ec2cluster", reflect.TypeOf(cluster))
	}
	return ec2cluster, nil
}

func TestInstanceAllocationRequest(t *testing.T) {
	var (
		cluster *Cluster
		err     error
	)
	// c5.large: mem:3.7GiB cpu:2 disk:10.0GiB intel_avx:2 intel_avx2:2 intel_avx512:2
	// c5.4xlarge: mem:32GiB cpu:16 disk:10.0GiB intel_avx:16 intel_avx2:16 intel_avx512:16
	// r3.8xlarge: mem:226.9GiB cpu:32 disk:10.0GiB intel_avx:32
	if cluster, err = getEC2ClusterWithRestrictedInstanceTypes(); err != nil {
		t.Fatal("ec2cluster: ", err)
	}
	for _, tc := range []struct {
		index   int
		waiters []*waiter
		reqs    []instanceConfig
	}{
		{
			1,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 3.7 * float64(data.GiB)}},
				}},
			[]instanceConfig{
				{
					Type: "c5.large",
				},
			},
		},
		// NOTE: 2 c5.large would have been to good in this case. But we try to find the biggest instance that fits everything.
		{
			2,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 3.7 * float64(data.GiB)}, Width: 2},
				}},
			[]instanceConfig{
				{
					Type: "c5.4xlarge",
				},
			},
		},
		{
			3,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 9 * float64(data.GiB)}, Width: 10},
				}},
			[]instanceConfig{
				{
					Type: "r3.8xlarge",
				},
			},
		},
		{
			4,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 32, "mem": 9 * float64(data.GiB)}, Width: 2},
				}},
			[]instanceConfig{
				{
					Type: "r3.8xlarge",
				},
				{
					Type: "r3.8xlarge",
				},
			},
		},
		{
			5,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 3.7 * float64(data.GiB)}},
				},
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 32, "mem": 9 * float64(data.GiB)}},
				}},
			[]instanceConfig{
				{
					Type: "c5.large",
				},
				{
					Type: "r3.8xlarge",
				},
			},
		},
		{ // NOTE: 6 One r3.8xlarge fits 4 of the 5-width requirement and a c5.4xlarge fits the remaining.
			6,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 3.7 * float64(data.GiB)}},
				},
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 8, "mem": float64(9 * data.GiB)}, Width: 5},
				}},
			[]instanceConfig{
				{
					Type: "r3.8xlarge",
				},
				{
					Type: "c5.4xlarge",
				},
			},
		},
		{ // NOTE: 7 We need a total of 2+15*2=32 cpu and mem, all of which fit in one r3.8xlarge.
			7,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 2 * float64(data.GiB)}},
				},
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 2 * float64(data.GiB)}, Width: 15},
				}},
			[]instanceConfig{
				{
					Type: "r3.8xlarge",
				},
			},
		},
		{
			8,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 33, "mem": 2 * float64(data.GiB)}},
				}},
			[]instanceConfig{},
		},
		{
			9,
			[]*waiter{
				{
					Requirements: reflow.Requirements{Min: reflow.Resources{"cpu": 1, "mem": 1 * float64(data.GiB)}, Width: 2},
				}},
			[]instanceConfig{
				{
					Type: "c5.large",
				},
			},
		},
	} {
		reqs := cluster.manager.getInstanceAllocations(tc.waiters)
		if got, want := len(reqs), len(tc.reqs); got != want {
			t.Fatalf("%v: expected %v allocation request, got %v", tc.index, want, got)
		}
		for i := range reqs {
			if got, want := reqs[i].Type, tc.reqs[i].Type; got != want {
				t.Fatalf("%v: expected %v, got %v", tc.index, want, got)
			}
		}
	}
}
