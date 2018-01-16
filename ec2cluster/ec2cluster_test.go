// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/state"
	"github.com/grailbio/internal/testutil"
)

func TestReconcile(t *testing.T) {
	instances := make(map[string]*ec2.Instance)
	for i := 0; i < 420; i++ {
		instances[fmt.Sprintf("i-%d", i)] = new(ec2.Instance)
	}
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	file, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Marshal(instances); err != nil {
		t.Fatal(err)
	}
	ec2 := new(mockEC2Client)
	cluster := &Cluster{
		EC2:  ec2,
		File: file,
	}
	if err := cluster.reconcile(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(ec2.DescribeInstancesInputs), 3; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	seen := make(map[string]bool)
	for i, input := range ec2.DescribeInstancesInputs {
		n := 200
		if i == 2 {
			n = 20
		}
		if got, want := len(input.Filters), 1; got != want {
			t.Errorf("got %d, want %d", got, want)
		}
		if got, want := len(input.Filters[0].Values), n; got != want {
			t.Errorf("[%d] got %d, want %d", i, got, want)
		}
		for _, id := range input.Filters[0].Values {
			seen[*id] = true
		}
	}
	if got, want := len(seen), 420; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	for id := range instances {
		delete(seen, id)
	}
	if got, want := len(seen), 0; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}
