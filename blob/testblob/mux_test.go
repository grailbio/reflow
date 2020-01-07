// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testblob

import (
	"context"
	"testing"

	"github.com/grailbio/reflow/blob"
)

func TestBucketKey(t *testing.T) {
	m := blob.Mux{"test": New("test")}
	for _, tc := range []struct {
		rawurl  string
		wantkey string
		wanterr bool
	}{
		{"test://bucket/Helloworld", "Helloworld", false},
		{"test://bucket/Hello #world #now", "Hello #world #now", false},
		{"test://bucket/tmp/reflow/modules/internal/grailrnar/summary_want.tsv?=2", "tmp/reflow/modules/internal/grailrnar/summary_want.tsv?=2", false},
	} {
		_, got, err := m.Bucket(context.Background(), tc.rawurl)
		if (err != nil) != tc.wanterr {
			t.Errorf("got %v want error %v", err, tc.wanterr)
		}
		if tc.wanterr {
			continue
		}
		if got != tc.wantkey {
			t.Errorf("got %v want %v", got, tc.wantkey)
		}
	}
}
