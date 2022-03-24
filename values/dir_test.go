package values

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/reflow"
)

var (
	f0 = reflow.File{}
	f1 = reflow.File{ID: reflow.Digester.FromString("f1 contents"), Size: 1}
	f2 = reflow.File{ID: reflow.Digester.FromString("f2 contents"), Size: 2}
	f3 = reflow.File{ID: reflow.Digester.FromString("f3 contents"), Size: 3}
	f4 = reflow.File{ID: reflow.Digester.FromString("f4 contents"), Size: 4}
	f5 = reflow.File{ID: reflow.Digester.FromString("f5 contents"), Size: 5}
	fMap = map[int]reflow.File {1: f1, 2: f2, 3: f3, 4: f4, 5: f5}
)

func TestMutableDir(t *testing.T) {
	contents := map[string]reflow.File{"a": f1, "b": f2, "d": f3, "c": f4}
	var md MutableDir
	for k, v := range contents {
		md.Set(k, v)
	}
	d := createDirFomMap(contents)
	if got, want := md.Dir(), d; !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSumDir(t *testing.T) {
	d1 := createDirFomMap(map[string]reflow.File{"c": f4, "a": f1, "b": f2})
	d2 := createDirFomMap(map[string]reflow.File{"d": f3, "c": f5})
	if got, want := d1.SortedKeys(), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := d2.SortedKeys(), []string{"c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	d := SumDir(d1, d2)

	if got, want := d.SortedKeys(), []string{"a", "b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, tt := range []struct {
		key  string
		want reflow.File
	}{
		{"a", f1}, {"b", f2}, {"c", f5}, {"d", f3}, {"e", f0},
	} {
		if got, _ := d.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}

	d = SumDir(d2, d1)
	for _, tt := range []struct {
		key  string
		want reflow.File
	}{
		{"a", f1}, {"b", f2}, {"c", f4}, {"d", f3}, {"e", f0},
	} {
		if got, _ := d.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestSumDirMany(t *testing.T) {
	dirs := []Dir{
		createDirFomMap(map[string]reflow.File{"c": f4}),
		createDirFomMap(map[string]reflow.File{"a": f1}),
		createDirFomMap(map[string]reflow.File{"b": f2}),
		createDirFomMap(map[string]reflow.File{"d": f3}),
		createDirFomMap(map[string]reflow.File{"c": f5}),
	}
	reducedD := ReduceUsingSumDir(dirs)
	singleD := createSingleDir(dirs)

	if got, want := reducedD.SortedKeys(), []string{"a", "b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := singleD.SortedKeys(), []string{"a", "b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, tt := range []struct {
		key  string
		want reflow.File
	}{
		{"a", f1}, {"b", f2}, {"c", f5}, {"d", f3}, {"e", f0},
	} {
		if got, _ := reducedD.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
		if got, _ := singleD.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestSumDirRandom(t *testing.T) {
	dirs := createTestDirs(100, 100, 20)
	singleD := createSingleDir(dirs)
	sumD := SumDirs(dirs)
	reducedD := ReduceUsingSumDir(dirs)
	if got, want := sumD.SortedKeys(), singleD.SortedKeys(); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := reducedD.SortedKeys(), singleD.SortedKeys(); !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot \t%v\nwant\t%v", got, want)
	}
	if got, want := len(singleD.contentsList), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	m := singleD.contentsList[0]
	for k, want := range m {
		if got, _ := sumD.Lookup(k); got != want {
			t.Errorf("sumD: got %v, want %v", got, want)
		}
		if got, _ := reducedD.Lookup(k); got != want {
			t.Errorf("reducedD: got %v, want %v", got, want)
		}
	}
	if got, want := singleD.Equal(&sumD), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := singleD.Equal(&reducedD), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestEqualDir(t *testing.T) {
	d1 := createDirFomMap(map[string]reflow.File{"c": f4, "a": f1, "b": f2})
	d2 := createDirFomMap(map[string]reflow.File{"d": f3, "c": f5})
	d3 := createDirFomMap(map[string]reflow.File{"d": f3, "c": f5, "a": f1, "b": f2})

	if got, want := d1.Equal(&d2), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := d1.Equal(&d3), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	d := SumDir(d1, d2) // must equal d3.
	if got, want := d.Equal(&d3), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	d = SumDir(d2, d1)
	if got, want := d.Equal(&d3), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

/*
goos: darwin
goarch: amd64
pkg: github.com/grailbio/reflow/values
cpu: Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz
BenchmarkSumTwoDirs
BenchmarkSumTwoDirs/SumDir_size_100
BenchmarkSumTwoDirs/SumDir_size_100-8         	  555762	      2079 ns/op	    3088 B/op	       2 allocs/op
BenchmarkSumTwoDirs/SumDirs_size_100
BenchmarkSumTwoDirs/SumDirs_size_100-8        	   35056	     33160 ns/op	    8178 B/op	       5 allocs/op
BenchmarkSumTwoDirs/SumDir_size_1000
BenchmarkSumTwoDirs/SumDir_size_1000-8        	   34630	     32420 ns/op	   28688 B/op	       2 allocs/op
BenchmarkSumTwoDirs/SumDirs_size_1000
BenchmarkSumTwoDirs/SumDirs_size_1000-8       	    2683	    503098 ns/op	  117440 B/op	       5 allocs/op
BenchmarkSumTwoDirs/SumDir_size_10000
BenchmarkSumTwoDirs/SumDir_size_10000-8       	    2115	    533932 ns/op	  286736 B/op	       2 allocs/op
BenchmarkSumTwoDirs/SumDirs_size_10000
BenchmarkSumTwoDirs/SumDirs_size_10000-8      	     184	   6734759 ns/op	  942144 B/op	       5 allocs/op
PASS
 */
func BenchmarkSumTwoDirs(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		for _, s := range []struct {
			name       string
			sumTwoDirsFunc func(d, e Dir) Dir
		}{
			{fmt.Sprintf("SumDir_size_%d", size), SumDir},
			{fmt.Sprintf("SumDirs_size_%d", size), func(d, e Dir) Dir { return SumDirs([]Dir{d, e})}},
		} {
			dirs := createTestDirs(2, size, 20)
			b.Run(s.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s.sumTwoDirsFunc(dirs[0], dirs[1])
				}
			})
		}
	}
}

/*
goos: goos: darwin
goarch: amd64
pkg: github.com/grailbio/reflow/values
cpu: Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz
BenchmarkSumManyDirs
BenchmarkSumManyDirs/ReduceNew_N_5_size_100
BenchmarkSumManyDirs/ReduceNew_N_5_size_100-8         	   64315	     19511 ns/op	   20984 B/op	       8 allocs/op
BenchmarkSumManyDirs/SumDirs_N_5_size_100
BenchmarkSumManyDirs/SumDirs_N_5_size_100-8           	   13040	     96884 ns/op	   28384 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_5_size_1000
BenchmarkSumManyDirs/ReduceNew_N_5_size_1000-8        	    2634	    437614 ns/op	  213112 B/op	       8 allocs/op
BenchmarkSumManyDirs/SumDirs_N_5_size_1000
BenchmarkSumManyDirs/SumDirs_N_5_size_1000-8          	     504	   2241472 ns/op	  245856 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_5_size_10000
BenchmarkSumManyDirs/ReduceNew_N_5_size_10000-8       	     175	   6057789 ns/op	 1925240 B/op	       8 allocs/op
BenchmarkSumManyDirs/SumDirs_N_5_size_10000
BenchmarkSumManyDirs/SumDirs_N_5_size_10000-8         	      36	  30916987 ns/op	 1985666 B/op	      24 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_10_size_100
BenchmarkSumManyDirs/ReduceNew_N_10_size_100-8        	   15345	     75762 ns/op	   74184 B/op	      18 allocs/op
BenchmarkSumManyDirs/SumDirs_N_10_size_100
BenchmarkSumManyDirs/SumDirs_N_10_size_100-8          	    5571	    205104 ns/op	   62848 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_10_size_1000
BenchmarkSumManyDirs/ReduceNew_N_10_size_1000-8       	    1282	    919473 ns/op	  741832 B/op	      18 allocs/op
BenchmarkSumManyDirs/SumDirs_N_10_size_1000
BenchmarkSumManyDirs/SumDirs_N_10_size_1000-8         	     432	   3057312 ns/op	  467072 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_10_size_10000
BenchmarkSumManyDirs/ReduceNew_N_10_size_10000-8      	      73	  26251204 ns/op	 7193032 B/op	      18 allocs/op
BenchmarkSumManyDirs/SumDirs_N_10_size_10000
BenchmarkSumManyDirs/SumDirs_N_10_size_10000-8        	      18	  69334113 ns/op	 3944498 B/op	      30 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_20_size_100
BenchmarkSumManyDirs/ReduceNew_N_20_size_100-8        	    3315	    369958 ns/op	  293960 B/op	      38 allocs/op
BenchmarkSumManyDirs/SumDirs_N_20_size_100
BenchmarkSumManyDirs/SumDirs_N_20_size_100-8          	    2608	    473464 ns/op	  117584 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_20_size_1000
BenchmarkSumManyDirs/ReduceNew_N_20_size_1000-8       	     252	   4994037 ns/op	 2782920 B/op	      38 allocs/op
BenchmarkSumManyDirs/SumDirs_N_20_size_1000
BenchmarkSumManyDirs/SumDirs_N_20_size_1000-8         	     171	   5867013 ns/op	  925904 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_100_size_100
BenchmarkSumManyDirs/ReduceNew_N_100_size_100-8       	     152	   8021118 ns/op	 6932335 B/op	     198 allocs/op
BenchmarkSumManyDirs/SumDirs_N_100_size_100
BenchmarkSumManyDirs/SumDirs_N_100_size_100-8         	     424	   2809187 ns/op	  467888 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_100_size_1000
BenchmarkSumManyDirs/ReduceNew_N_100_size_1000-8      	       9	 121089403 ns/op	65361128 B/op	     198 allocs/op
BenchmarkSumManyDirs/SumDirs_N_100_size_1000
BenchmarkSumManyDirs/SumDirs_N_100_size_1000-8        	      26	  39474082 ns/op	 3933424 B/op	       7 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_500_size_10
BenchmarkSumManyDirs/ReduceNew_N_500_size_10-8        	      64	  18230010 ns/op	18437486 B/op	     998 allocs/op
BenchmarkSumManyDirs/SumDirs_N_500_size_10
BenchmarkSumManyDirs/SumDirs_N_500_size_10-8          	     892	   1177668 ns/op	  241712 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_1000_size_10
BenchmarkSumManyDirs/ReduceNew_N_1000_size_10-8       	      16	  68980686 ns/op	72250775 B/op	    1999 allocs/op
BenchmarkSumManyDirs/SumDirs_N_1000_size_10
BenchmarkSumManyDirs/SumDirs_N_1000_size_10-8         	     408	   2545328 ns/op	  475184 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_2000_size_10
BenchmarkSumManyDirs/ReduceNew_N_2000_size_10-8       	       4	 261225092 ns/op	282370692 B/op	    4002 allocs/op
BenchmarkSumManyDirs/SumDirs_N_2000_size_10
BenchmarkSumManyDirs/SumDirs_N_2000_size_10-8         	     200	   5655592 ns/op	  942128 B/op	       5 allocs/op
BenchmarkSumManyDirs/ReduceNew_N_5000_size_10
BenchmarkSumManyDirs/ReduceNew_N_5000_size_10-8       	       1	1864878714 ns/op	1734926000 B/op	    9999 allocs/op
BenchmarkSumManyDirs/SumDirs_N_5000_size_10
BenchmarkSumManyDirs/SumDirs_N_5000_size_10-8         	      68	  18226049 ns/op	 2015309 B/op	       5 allocs/op
PASS
 */
func BenchmarkSumManyDirs(b *testing.B) {
	for _, bb := range []struct{ n, size int }{
		{5, 100},
		{5, 1000},
		{5, 10000},
		{10, 100},
		{10, 1000},
		{10, 10000},
		{20, 100},
		{20, 1000},
		{100, 100},
		{100, 1000},
		{500, 10},
		{1000, 10},
		{2000, 10},
		{5000, 10},
	} {
		for _, s := range []struct {
			name       string
			sumDirFunc func(dirs []Dir) Dir
		}{
			{fmt.Sprintf("ReduceNew_N_%d_size_%d", bb.n, bb.size), ReduceUsingSumDir},
			{fmt.Sprintf("SumDirs_N_%d_size_%d", bb.n, bb.size), SumDirs},
		} {
			dirs := createTestDirs(bb.n, bb.size, 20)
			b.Run(s.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s.sumDirFunc(dirs)
				}
			})
		}
	}
}

func createDirFomMap(m map[string]reflow.File) Dir {
	var md MutableDir
	for k, v := range m {
		md.Set(k, v)
	}
	return md.Dir()
}

// createSingleDir creates a single dir from the given list of dirs,
// using a MutableDir such that internally it contains a single map
// with all the values from all of the dirs (and their lists of maps)
func createSingleDir(dirs []Dir) Dir {
	var md MutableDir
	for _, e := range dirs {
		for _, c := range e.contentsList {
			for k, v := range c {
				md.Set(k, v)
			}
		}
	}
	return md.Dir()
}

// createTestDirs creates n Dir each of which contains size elements and
// dupProbPct is the percent chance of a duplicate key being used.
func createTestDirs(n, size, dupProbPct int) []Dir {
	rand.Seed(time.Now().UnixNano())
	dirs := make([]Dir, 0, n)
	var duplicates []string
	for i := 0; i < size/10; i++ {
		duplicates = append(duplicates, randString())
	}
	for i := 0; i < n; i++ {
		var md MutableDir
		for j := 0; j < size; j++ {
			var key string
			if rand.Intn(100) < dupProbPct && len(duplicates) > 0 {
				// 20% of the time, use a duplicate key.
				key = duplicates[rand.Intn(len(duplicates))]
			} else {
				key = randString()
			}
			md.Set(key, randFile())
		}
		dirs = append(dirs, md.Dir())
	}
	return dirs
}

func randFile() reflow.File {
	return fMap[1 + rand.Intn(5)]
}

const minLen = 10
const maxLen = 100
const chars = "abcdefghijklmnopqrstuvwxyxABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString() string {
	l := minLen + rand.Intn(maxLen-minLen+1)
	b := make([]byte, l)
	for i := 0; i < l; i++ {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
