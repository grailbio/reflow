package fmt

import (
	"context"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil/assert"
	"path"
	"github.com/grailbio/reflow/syntax"
	"bytes"
	"bufio"
)

// TestWriteModule formats a reflow module and checks the result for byte equality.
// All format changes will likely require an update to the test fixtures. The test
// fixtures provide a good example of most formatter behavior.
func TestWriteModule(t *testing.T) {
	ctx := context.Background()

	lister := file.List(ctx, "testdata/unformatted/", false)
	for lister.Scan() {
		assert.NoError(t, lister.Err())
		assert.False(t, lister.IsDir())

		srcPath := lister.Path()
		srcPathBase := path.Base(srcPath)
		assert.EQ(t, path.Ext(srcPathBase), ".rf")
		testId := srcPathBase[:len(srcPathBase)-3]

		t.Run(testId, func(t *testing.T) {
			srcBytes, err := file.ReadFile(ctx, srcPath)
			assert.NoError(t, err, srcPath)

			srcParser := &syntax.Parser{
				File: srcPath,
				Body: bytes.NewReader(srcBytes),
				Mode: syntax.ParseModule,
			}
			assert.NoError(t, srcParser.Parse(), srcPath)

			out := new(bytes.Buffer)
			bw := bufio.NewWriter(out)
			NewWithOpts(bw, Opts{DebugWritePos:true, MaxBlankLines:10}).WriteModule(srcParser.Module)
			bw.Flush()
			fmtBytes := out.Bytes()

			//fmtParser := &syntax.Parser{
			//	File: srcPath,
			//	Body: bytes.NewReader(fmtBytes),
			//	Mode: syntax.ParseModule,
			//}
			//assert.NoError(t, fmtParser.Parse())

			expectedPath := path.Join("testdata", "formatted", path.Base(srcPath))
			expectedBytes, err := file.ReadFile(ctx, expectedPath)
			assert.NoError(t, err, srcPath)

			//if string(fmtBytes) != string(expectedBytes) {
			//	i := commonLength(fmtBytes, expectedBytes)
			//	t.Errorf("%s!!!!!!!!!!%s", string(fmtBytes[:i]), string(fmtBytes[i:]))
			//	t.Fail()
			//}
			assert.EQ(t, string(fmtBytes), string(expectedBytes))
		})
	}
}

func commonLength(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}
