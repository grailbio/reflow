package tool

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/test/testutil"

	"github.com/stretchr/testify/require"
)

func TestClauses(t *testing.T) {
	for _, test := range []struct {
		re     string
		labels []string
		match  bool
	}{
		{`label=alpha`, []string{`label=alpha`}, true},
		{`!label=alpha`, []string{`label=alpha`}, false},
		{`label=beta`, []string{`label=alpha`}, false},

		{`label=alpha label=beta`, []string{`label=alpha`}, true},
		{`label=alpha label=beta`, []string{`label=beta`}, true},
		{`label=alpha label=beta`, []string{`label=gamma`}, false},

		{`label=alpha,label=beta`, []string{`label=alpha`}, false},
		{`label=alpha,label=beta`, []string{`label=alpha`, `label=beta`}, true},
		{`label=alpha,label=beta`, []string{`label=alpha`, `label=beta`, `label=gamma`}, true},
		{`label=alpha,label=beta label=alpha`, []string{`label=alpha`}, true},
		{`label=alpha label=alpha,label=beta`, []string{`label=alpha`}, true},

		{`label=alpha !label=beta`, []string{`label=alpha`, `label=beta`}, true},
		{`label=alpha !label=beta`, []string{`label=alpha`, `label=gamma`}, true},
		{`label=alpha !label=beta`, []string{`label=gamma`, `label=phi`}, true},
		{`label=alpha !label=beta`, []string{`label=gamma`, `label=beta`}, false},

		{`label=alpha,!label=beta`, []string{`label=alpha`, `label=beta`}, false},
		{`label=alpha,!label=beta`, []string{`label=alpha`, `label=gamma`}, true},
		{`!label=alpha,!label=beta`, []string{`label=gamma`}, true},
		{`!label=alpha,!label=beta`, []string{`label=alpha`}, false},
		{`!label=alpha,!label=beta`, []string{`label=beta`}, false},
	} {
		filter, err := parseFilter(test.re)
		if err != nil {
			t.Fatal(err)
		}
		if got, expected := filter.Match(test.labels), test.match; got != expected {
			t.Errorf("filter:%v, labels:%v expected %v, got %v", test.re, test.labels, expected, got)
		}
	}
}

func nullInps(inps *collectInputs) {
	inps.keyFilter = nil
	inps.valueFilter = nil
	inps.deadKeyFilter = nil
	inps.deadValueFilter = nil
}

func TestBuildCollectInputsAndMigrate(t *testing.T) {
	// setup the assoc and repo with:
	// 2 x FS2 filesets with 2 files each
	// 2 x FS1 filesets with 2 files each

	fuzz := testutil.NewFuzz(nil)
	ctx := context.Background()
	ass, repo := testutil.NewInmemoryAssoc(), testutil.NewInmemoryRepository("")
	for i := 0; i < 2; i++ {
		k := fuzz.Digest()
		fs := fuzz.FilesetDeep(2, 0, false, false)
		var v digest.Digest
		var err error
		if v, err = repository.Marshal(ctx, repo, &fs); err != nil {
			t.Fatal(err)
		}
		if err = ass.Store(ctx, assoc.FilesetV2, k, v); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%v -> %v (%s)\n", k, v, fs.String())
	}

	for i := 0; i < 2; i++ {
		k := fuzz.Digest()
		fs := testutil.WriteFiles(repo, fuzz.String(" "), fuzz.String(" "))
		buf := new(bytes.Buffer)
		wErr := fs.Write(buf, assoc.Fileset, true, true)
		require.NoError(t, wErr)
		v1Digest, pErr := repo.Put(ctx, buf)
		require.NoError(t, pErr)
		if err := ass.Store(ctx, assoc.Fileset, k, v1Digest); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name                string
		times               []time.Time
		labels              [][]string
		keepFilterPattern   string
		labelsFilterPattern string
		threshold           time.Time
		maxFS2MigrateCount  int64
		expectedResult      *collectInputs
	}{
		{
			"collect all by threshold",
			[]time.Time{
				time.Unix(100, 0),
				time.Unix(100, 0),
				time.Unix(100, 0),
				time.Unix(100, 0),
			},
			[][]string{
				{"foo"},
				{"foo"},
				{"foo"},
				{"foo"},
			},
			"bar",
			"",
			time.Unix(50, 0),
			0,
			&collectInputs{
				itemsScannedCount:          4,
				liveObjectsInFilesets:      0,
				liveItemCount:              0,
				liveObjectsNotInRepository: 0,
			},
		},
		{
			"collect half by threshold",
			[]time.Time{
				time.Unix(100, 0),
				time.Unix(100, 0),
				time.Unix(200, 0),
				time.Unix(200, 0),
			},
			[][]string{
				{"foo"},
				{"foo"},
				{"foo"},
				{"foo"},
			},
			"bar",
			"bar",
			time.Unix(150, 0),
			0,
			&collectInputs{
				itemsScannedCount:          4,
				liveObjectsInFilesets:      2 * 2,
				liveItemCount:              2,
				liveObjectsNotInRepository: 0,
			},
		},
		{
			"collect half by labels",
			[]time.Time{
				time.Unix(200, 0),
				time.Unix(200, 0),
				time.Unix(200, 0),
				time.Unix(200, 0),
			},
			[][]string{
				{"foo"},
				{"foo"},
				{"buzz"},
				{"buzz"},
			},
			"bar",
			"foo",
			time.Unix(100, 0),
			0,
			&collectInputs{
				itemsScannedCount:          4,
				liveObjectsInFilesets:      2 * 2,
				liveItemCount:              2,
				liveObjectsNotInRepository: 0,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tAss, tRepo := ass.Copy(), repo.Copy()
			var timesIdx, labelsIdx int
			tAss.SetScanTimeGenerator(func() time.Time {
				v := test.times[timesIdx]
				timesIdx++
				return v
			})
			tAss.SetScanLabelsGenerator(func() []string {
				v := test.labels[labelsIdx]
				labelsIdx++
				return v
			})

			keepFilter, err := parseFilter(test.keepFilterPattern)
			require.NoError(t, err)

			labelsFilter, err := parseFilter(test.labelsFilterPattern)
			require.NoError(t, err)

			inps, err := (&Cmd{}).buildCollectInputs(ctx, tAss, tRepo, keepFilter, labelsFilter, test.threshold)
			require.NoError(t, err)
			nullInps(inps)
			require.Equal(t, test.expectedResult, inps)
		})
	}
}
