// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"io/ioutil"
	"math/big"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/walker"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

type systemFunc struct {
	Module string
	Id     string
	Doc    string
	Type   *types.T
	Force  bool
	Do     func(loc values.Location, args []values.T) (values.T, error)
}

func (s systemFunc) Apply(loc values.Location, args []values.T) (values.T, error) {
	args = append([]values.T{}, args...)
	var (
		deps  []*reflow.Flow
		depsi []int
		dw    = reflow.Digester.NewWriter()
	)
	for i := range args {
		if s.Force {
			args[i] = Force(args[i], s.Type.Fields[i].T)
		}
		if f, ok := args[i].(*reflow.Flow); ok {
			deps = append(deps, f)
			depsi = append(depsi, i)
		} else {
			values.WriteDigest(dw, args[i], s.Type.Fields[i].T)
		}
	}
	if len(deps) == 0 {
		return s.Do(loc, args)
	}
	digest.WriteDigest(dw, s.Digest())
	return &reflow.Flow{
		Op:         reflow.OpK,
		Deps:       deps,
		FlowDigest: dw.Digest(),
		Position:   loc.Position,
		Ident:      loc.Ident,
		K: func(vs []values.T) *reflow.Flow {
			for i := range vs {
				args[depsi[i]] = vs[i]
			}
			rv, err := s.Do(loc, args)
			if err != nil {
				return &reflow.Flow{Op: reflow.OpVal, Err: errors.Recover(err)}
			}
			return flow(rv, s.Type.Elem)
		},
	}, nil
}

func (s systemFunc) Digest() digest.Digest {
	return reflow.Digester.FromString("$/" + s.Module + s.Id)
}

func (s systemFunc) Decl() *Decl {
	return &Decl{
		Kind:    DeclAssign,
		Comment: s.Doc,
		Pat:     &Pat{Kind: PatIdent, Ident: s.Id},
		Expr:    &Expr{Kind: ExprConst, Val: s, Type: s.Type},
		Type:    s.Type,
	}
}

// Stdlib returns the type and value environments for reflow's
// standard library.
func Stdlib() (*types.Env, *values.Env) {
	var (
		tenv = types.NewEnv()
		venv = values.NewEnv()
	)
	define := func(sym, doc string, t *types.T, v values.T) {
		tenv.Bind(sym, t)
		venv.Bind(sym, v)
	}

	funcs := []systemFunc{
		{
			Id:   "file",
			Type: types.Func(types.File, &types.Field{"url", types.String}),
			Do: func(loc values.Location, vs []values.T) (values.T, error) {
				rawurl := strings.TrimRight(vs[0].(string), "/")
				u, err := url.Parse(rawurl)
				if err != nil {
					return nil, err
				}
				if u.Scheme == "" {
					// This is a (small) local file; we inline it as a literal.
					b, err := ioutil.ReadFile(rawurl)
					if err != nil {
						return nil, err
					}
					if len(b) > 200<<20 {
						return nil, fmt.Errorf("file %s is too large (%dMB); local files may not exceed 200MB", rawurl, len(b)>>20)
					}
					return &reflow.Flow{
						Deps: []*reflow.Flow{{
							Op:       reflow.OpData,
							Data:     b,
							Position: loc.Position,
							Ident:    loc.Ident,
						}},
						FlowDigest: reflow.Digester.FromString("file.fs$file1"),
						Op:         reflow.OpCoerce,
						Coerce: func(v values.T) (values.T, error) {
							fs := v.(reflow.Fileset)
							f, ok := fs.Map["."]
							if !ok {
								return nil, errors.E("file", u.String(), errors.NotExist)
							}
							return values.File(f), nil
						},
					}, nil
				}

				return &reflow.Flow{
					Deps: []*reflow.Flow{{
						Op:       reflow.OpIntern,
						URL:      u,
						Position: loc.Position,
						Ident:    loc.Ident,
					}},
					FlowDigest: reflow.Digester.FromString("file.fs$file"),
					Op:         reflow.OpCoerce,
					Coerce: func(v values.T) (values.T, error) {
						fs := v.(reflow.Fileset)
						f, ok := fs.Map["."]
						if !ok {
							return nil, errors.E("file", u.String(), errors.NotExist)
						}
						return values.File(f), nil
					},
				}, nil
			},
		},
		{
			Id:   "dir",
			Type: types.Func(types.Dir, &types.Field{"url", types.String}),
			Do: func(loc values.Location, vs []values.T) (values.T, error) {
				rawurl := strings.TrimRight(vs[0].(string), "/") + "/"
				u, err := url.Parse(rawurl)
				if err != nil {
					return nil, err
				}
				if u.Scheme == "" {
					// Take this to be a local directory of (small) files.
					var total int64
					const maxTotal = 100 << 20
					var w walker.Walker
					w.Init(rawurl)
					var paths []string
					var datas [][]byte
					for w.Scan() {
						info := w.Info()
						if !info.IsDir() {
							total += info.Size()
							if total > maxTotal {
								return nil, fmt.Errorf("directory %s exceeds maximum size of 100MB", rawurl)
							}
						} else {
							continue
						}
						paths = append(paths, w.Relpath())
						b, err := ioutil.ReadFile(w.Path())
						if err != nil {
							return nil, err
						}
						datas = append(datas, b)
					}
					if len(datas) == 0 {
						return nil, fmt.Errorf("empty directory %s", rawurl)
					}
					dataFlows := make([]*reflow.Flow, len(datas))
					for i := range datas {
						dataFlows[i] = &reflow.Flow{
							Op:       reflow.OpData,
							Data:     datas[i],
							Position: loc.Position,
							Ident:    loc.Ident,
						}
					}
					return &reflow.Flow{
						Deps:       dataFlows,
						FlowDigest: reflow.Digester.FromString("file.fs$file2"),
						Op:         reflow.OpK,
						Position:   loc.Position,
						Ident:      loc.Ident,
						K: func(vs []values.T) *reflow.Flow {
							dir := make(values.Dir)
							for i := range vs {
								dir[paths[i]] = values.File(vs[i].(reflow.Fileset).Map["."])
							}
							return &reflow.Flow{
								Op:         reflow.OpVal,
								Value:      dir,
								FlowDigest: values.Digest(dir, types.Dir),
							}
						},
					}, nil
				}
				return &reflow.Flow{
					Deps: []*reflow.Flow{{
						Op:       reflow.OpIntern,
						URL:      u,
						Position: loc.Position,
						Ident:    loc.Ident,
					}},
					Op:         reflow.OpCoerce,
					FlowDigest: reflow.Digester.FromString("$dir.fs2dir"),
					Coerce: func(v values.T) (values.T, error) {
						fs := v.(reflow.Fileset)
						dir := make(values.Dir)
						for k, v := range fs.Map {
							dir[k] = values.File(v)
						}
						return dir, nil
					},
				}, nil
			},
		},
	}

	for _, f := range funcs {
		define(f.Id, f.Doc, f.Type, f)
	}
	define("KiB", "one kibibyte", types.Int, big.NewInt(1<<10))
	define("MiB", "one mebibyte", types.Int, big.NewInt(1<<20))
	define("GiB", "one gigibyte", types.Int, big.NewInt(1<<30))
	define("TiB", "one tebibyte", types.Int, big.NewInt(1<<40))

	return tenv, venv
}

var lib = map[string]*Module{}

var testDecls = []*Decl{
	systemFunc{
		Id:     "Assert",
		Module: "test",
		Doc:    "Assert fails if any passed (boolean) value is false.",
		Type:   types.Func(types.Unit, &types.Field{Name: "tests", T: types.List(types.Bool)}),
		Force:  true,
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			list := args[0].(values.List)
			var failed []int
			for i, e := range list {
				if !e.(bool) {
					failed = append(failed, i)
				}
			}
			if len(failed) > 0 {
				return nil, fmt.Errorf("failed assertions: %v", failed)
			}
			return values.Unit, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "All",
		Module: "test",
		Doc:    "All returns true if every passed (boolean) value is true.",
		Type:   types.Func(types.Bool, &types.Field{Name: "tests", T: types.List(types.Bool)}),
		Force:  true,
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			list := args[0].(values.List)
			for i := range list {
				if !list[i].(bool) {
					return false, nil
				}
			}
			return true, nil
		},
	}.Decl(),
}

var dirsDecls = []*Decl{
	systemFunc{
		Id:     "Groups",
		Module: "dirs",
		Doc: "Groups assigns each path in a directory to a group according " +
			"to the passed-in regular expressio, which must have exactly one " +
			"regexp group. Paths that do not match the expression are filtered out. " +
			"Group returns a map that maps each group key to a directory of matched values.",
		Type: types.Func(types.Map(types.String, types.Dir),
			&types.Field{Name: "dir", T: types.Dir},
			&types.Field{Name: "re", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			dir, raw := args[0].(values.Dir), args[1].(string)
			re, err := regexp.Compile(raw)
			if err != nil {
				return nil, err
			}
			groups := map[string]values.Dir{}
			for path, file := range dir {
				idx := re.FindStringSubmatch(path)
				if len(idx) != 2 {
					continue
				}
				v, ok := groups[idx[1]]
				if !ok {
					groups[idx[1]] = make(values.Dir)
					v = groups[idx[1]]
				}
				v[path] = file
			}
			v := make(values.Map)
			for key, group := range groups {
				v[key] = group
			}
			return v, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Make",
		Module: "dirs",
		Force:  true,
		Doc:    "Make creates a new dir using the given map of paths to files.",
		Type: types.Func(types.Dir,
			&types.Field{Name: "map", T: types.Map(types.String, types.File)}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			m := args[0].(values.Map)
			dir := make(values.Dir)
			for path, file := range m {
				dir[path.(string)] = file.(values.File)
			}
			return dir, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Pick",
		Module: "dirs",
		Doc: "Pick returns the first file in a directory matching a glob pattern. " +
			"Pick fails if no files match.",
		Type: types.Func(types.Tuple(&types.Field{T: types.File}, &types.Field{T: types.String}),
			&types.Field{Name: "dir", T: types.Dir},
			&types.Field{Name: "pattern", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			dir, pat := args[0].(values.Dir), args[1].(string)
			for key, file := range dir {
				ok, err := path.Match(pat, key)
				if err != nil {
					return nil, err
				}
				if ok {
					return values.Tuple{file, key}, nil
				}
			}
			return nil, errors.New("no files matched")
		},
	}.Decl(),
	systemFunc{
		Id:     "Files",
		Module: "dirs",
		Doc:    "Files returns a sorted (by filename) list of files from a directory.",
		Type: types.Func(types.List(types.File),
			&types.Field{Name: "dir", T: types.Dir}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			dir := args[0].(values.Dir)
			var keys []string
			for key := range dir {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			files := make(values.List, len(keys))
			for i, key := range keys {
				files[i] = dir[key]
			}
			return files, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Copy",
		Module: "dirs",
		Doc:    "Copy copies the directory to an extern location.",
		Type: types.Func(types.Unit,
			&types.Field{Name: "dir", T: types.Dir},
			&types.Field{Name: "url", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			dir, rawurl := args[0].(values.Dir), args[1].(string)
			rawurl = strings.TrimRight(rawurl, "/") + "/"
			u, err := url.Parse(rawurl)
			if err != nil {
				return nil, err
			}
			if u.Scheme == "" {
				return nil, fmt.Errorf("dirs.Copy: scheme not provided in destination url %s", rawurl)
			}
			return &reflow.Flow{
				Op:       reflow.OpExtern,
				Position: loc.Position,
				Ident:    loc.Ident,
				Deps:     []*reflow.Flow{{Op: reflow.OpVal, Value: dirToFileset(dir)}},
				URL:      u,
			}, nil
		},
	}.Decl(),
}

var filesDecls = []*Decl{
	systemFunc{
		Id:     "Copy",
		Module: "files",
		Doc:    "Copy copies the file to an extern location.",
		Type: types.Func(types.Unit,
			&types.Field{Name: "file", T: types.File},
			&types.Field{Name: "url", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			file, rawurl := args[0].(values.File), args[1].(string)
			rawurl = strings.TrimRight(rawurl, "/")
			u, err := url.Parse(rawurl)
			if err != nil {
				return nil, err
			}
			if u.Scheme == "" {
				return nil, fmt.Errorf("files.Copy: scheme not provided in destination url %s", rawurl)
			}
			return &reflow.Flow{
				Op:       reflow.OpExtern,
				Position: loc.Position,
				Ident:    loc.Ident,
				Deps:     []*reflow.Flow{{Op: reflow.OpVal, Value: fileToFileset(file)}},
				URL:      u,
			}, nil
		},
	}.Decl(),
}

var regexpDecls = []*Decl{
	systemFunc{
		Id:     "Groups",
		Module: "regexp",
		Doc: "Groups matches a string with a regular expression and returns a list " +
			"containing the matched groups. Groups fails if the string does not match " +
			"the regular expression.",
		Type: types.Func(types.List(types.String),
			&types.Field{Name: "str", T: types.String},
			&types.Field{Name: "regexp", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			str, raw := args[0].(string), args[1].(string)
			re, err := regexp.Compile(raw)
			if err != nil {
				return nil, err
			}
			groups := re.FindStringSubmatch(str)
			if groups == nil {
				return nil, fmt.Errorf("regexp %s does not match string %s", raw, str)
			}
			list := make(values.List, len(groups)-1)
			for i := range list {
				list[i] = groups[i+1]
			}
			return list, nil
		},
	}.Decl(),
}

var stringsDecls = []*Decl{
	systemFunc{
		Id:     "Split",
		Module: "strings",
		Doc:    "Split splits the string s by separator sep.",
		Type: types.Func(types.List(types.String),
			&types.Field{Name: "s", T: types.String},
			&types.Field{Name: "sep", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			s, sep := args[0].(string), args[1].(string)
			strs := strings.Split(s, sep)
			list := make(values.List, len(strs))
			for i := range strs {
				list[i] = strs[i]
			}
			return list, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Sort",
		Module: "strings",
		Doc:    "Sort sorts a list of strings in lexicographic order.",
		Type: types.Func(types.List(types.String),
			&types.Field{Name: "strs", T: types.List(types.String)}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			list := args[0].(values.List)
			sorted := make(values.List, len(list))
			copy(sorted, list)
			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].(string) < sorted[j].(string)
			})
			return sorted, nil
		},
	}.Decl(),
}

var pathDecls = []*Decl{
	systemFunc{
		Id:     "Base",
		Module: "path",
		Doc:    "Base returns the last element of path.",
		Type: types.Func(types.String,
			&types.Field{Name: "path", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			return path.Base(args[0].(string)), nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Dir",
		Module: "path",
		Doc: "Dir returns all but the last element of path. The result is " +
			"not cleaned, and thus remains compatible with URL inputs",
		Type: types.Func(types.String,
			&types.Field{Name: "path", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			// We don't use Go's path.Dir here because we want these
			// to be compatible with URLs also.
			dir, _ := path.Split(args[0].(string))
			if dir != "/" {
				dir = strings.TrimSuffix(dir, "/")
			}
			return dir, nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Ext",
		Module: "path",
		Doc:    "Ext returns the file name extension of path.",
		Type: types.Func(types.String,
			&types.Field{Name: "path", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			return path.Ext(args[0].(string)), nil
		},
	}.Decl(),
	systemFunc{
		Id:     "Join",
		Module: "path",
		Doc: "Join joins a number of path elements into a single path. " +
			"Empty elements are ignored, but the result is otherwise not cleaned " +
			"and is thus compatible with URLs.",
		Type: types.Func(types.String,
			&types.Field{Name: "paths", T: types.List(types.String)}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			// We don't use Go's path.Join here because we want these
			// to be compatible with URLs also.
			list := args[0].(values.List)
			var elems []string
			for _, v := range list {
				if s := v.(string); s != "" {
					elems = append(elems, s)
				}
			}
			return strings.Join(elems, "/"), nil
		},
	}.Decl(),
}

func init() {
	for _, mod := range []struct {
		name  string
		decls []*Decl
	}{
		{"test", testDecls},
		{"dirs", dirsDecls},
		{"files", filesDecls},
		{"regexp", regexpDecls},
		{"strings", stringsDecls},
		{"path", pathDecls},
	} {
		lib[mod.name] = &Module{Decls: mod.decls}
		lib[mod.name].Init(nil, types.NewEnv())
	}
}
