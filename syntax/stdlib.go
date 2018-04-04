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
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/walker"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// SystemFunc is utility to define a reflow intrinsic.
type SystemFunc struct {
	Module string
	Id     string
	Doc    string
	Type   *types.T
	Force  bool
	Do     func(loc values.Location, args []values.T) (values.T, error)
}

// Apply applied the intrinsic with the given arguments.
func (s SystemFunc) Apply(loc values.Location, args []values.T) (values.T, error) {
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

// Digest computes the digest of the intrinsic.
func (s SystemFunc) Digest() digest.Digest {
	return reflow.Digester.FromString("$/" + s.Module + s.Id)
}

// Decl returns the intrinsic as a reflow declaration.
func (s SystemFunc) Decl() *Decl {
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

	funcs := []SystemFunc{
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
					const maxTotal = 200 << 20
					var w walker.Walker
					w.Init(rawurl)
					var paths []string
					var datas [][]byte
					for w.Scan() {
						info := w.Info()
						if !info.IsDir() {
							total += info.Size()
							if total > maxTotal {
								return nil, fmt.Errorf("directory %s exceeds maximum size of 200MB", rawurl)
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

var (
	mu  sync.Mutex
	lib = map[string]*ModuleImpl{}
)

// Modules returns the names of the available systems modules.
func Modules() (names []string) {
	mu.Lock()
	for name := range lib {
		names = append(names, name)
	}
	mu.Unlock()
	return names
}

var testDecls = []*Decl{
	SystemFunc{
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
	SystemFunc{
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

var coerceFilesetToDirDigest = reflow.Digester.FromString("grail.com/reflow/syntax.coerceFilesetToDir")

func coerceFilesetToDir(v values.T) (values.T, error) {
	fs := v.(reflow.Fileset)
	dir := make(values.Dir)
	for key, file := range fs.Map {
		dir[key] = values.File(file)
	}
	return dir, nil
}

var dirsDecls = []*Decl{
	SystemFunc{
		Id:     "Groups",
		Module: "dirs",
		Doc: "Groups assigns each path in a directory to a group according " +
			"to the passed-in regular expression, which must have exactly one " +
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
			m := make(values.Map)
			for key, group := range groups {
				m.Insert(values.Digest(key, types.String), key, group)
			}
			return m, nil
		},
	}.Decl(),
	SystemFunc{
		Id:     "Make",
		Module: "dirs",
		Force:  true,
		Doc:    "Make creates a new dir using the given map of paths to files.",
		Type: types.Func(types.Dir,
			&types.Field{Name: "map", T: types.Map(types.String, types.File)}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			m := args[0].(values.Map)
			dir := make(values.Dir)
			m.Each(func(path, file values.T) {
				dir[path.(string)] = file.(values.File)
			})
			return dir, nil
		},
	}.Decl(),
	SystemFunc{
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
			return nil, errors.Errorf("dirs.Pick: no files matched %s", pat)
		},
	}.Decl(),
	SystemFunc{
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
	SystemFunc{
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
	SystemFunc{
		Id:     "Fileset",
		Module: "dirs",
		Doc:    "Fileset coerces a fileset into a dir.",
		Type:   types.Func(types.Dir, &types.Field{Name: "fileset", T: types.Fileset}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			if f, ok := args[0].(*reflow.Flow); ok {
				return &reflow.Flow{
					Op:         reflow.OpCoerce,
					Deps:       []*reflow.Flow{f},
					FlowDigest: coerceFilesetToDirDigest,
					Coerce:     coerceFilesetToDir,
				}, nil
			}
			return coerceFilesetToDir(args[0])
		},
	}.Decl(),
}

var coerceFilesetToFileDigest = reflow.Digester.FromString("grail.com/reflow/syntax.coerceFilesetToFile")

func coerceFilesetToFile(v values.T) (values.T, error) {
	fs := v.(reflow.Fileset)
	f, ok := fs.Map["."]
	if !ok {
		return nil, errors.Errorf("files.Fileset: invalid fileset %v", fs)
	}
	return values.File(f), nil
}

var filesDecls = []*Decl{
	SystemFunc{
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
	SystemFunc{
		Id:     "Fileset",
		Module: "files",
		Doc:    "Fileset coerces a fileset into a file.",
		Type:   types.Func(types.File, &types.Field{Name: "file", T: types.Fileset}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			if f, ok := args[0].(*reflow.Flow); ok {
				return &reflow.Flow{
					Op:         reflow.OpCoerce,
					Deps:       []*reflow.Flow{f},
					FlowDigest: coerceFilesetToFileDigest,
					Coerce:     coerceFilesetToFile,
				}, nil
			}
			return coerceFilesetToFile(args[0])
		},
	}.Decl(),
}

var regexpDecls = []*Decl{
	SystemFunc{
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
	SystemFunc{
		Id:     "Match",
		Module: "regexp",
		Doc:    "Match checks whether a regular expression matches a string.",
		Type: types.Func(types.Bool,
			&types.Field{Name: "str", T: types.String},
			&types.Field{Name: "regexp", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			str, raw := args[0].(string), args[1].(string)
			return regexp.MatchString(raw, str)
		},
	}.Decl(),

	SystemFunc{
		Id:     "Replace",
		Module: "regexp",
		Doc: "Replace returns a copy of src, replacing matches of the regular " +
			"expression (if any) with the replacement string. Semantics are same " +
			"as Go's regexp.ReplaceAllString.",
		Type: types.Func(types.String,
			&types.Field{Name: "src", T: types.String},
			&types.Field{Name: "regexp", T: types.String},
			&types.Field{Name: "repl", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			src, raw, repl := args[0].(string), args[1].(string), args[2].(string)
			re, err := regexp.Compile(raw)
			if err != nil {
				return nil, err
			}
			return re.ReplaceAllString(src, repl), nil
		},
	}.Decl(),
}

var stringsDecls = []*Decl{
	SystemFunc{
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
	SystemFunc{
		Id:     "Join",
		Module: "strings",
		Doc:    "Join concatenates a list of strings into a single string using the provided separator.",
		Type: types.Func(types.String,
			&types.Field{Name: "strs", T: types.List(types.String)},
			&types.Field{Name: "sep", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			strs, sep := args[0].(values.List), args[1].(string)
			gostrs := make([]string, len(strs))
			for i := range gostrs {
				gostrs[i] = strs[i].(string)
			}
			return strings.Join(gostrs, sep), nil
		},
	}.Decl(),
	SystemFunc{
		Id:     "HasPrefix",
		Module: "strings",
		Doc:    "HasPrefix tests whether the string s begins with prefix.",
		Type: types.Func(types.Bool,
			&types.Field{Name: "s", T: types.String},
			&types.Field{Name: "prefix", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			s, prefix := args[0].(string), args[1].(string)
			return strings.HasPrefix(s, prefix), nil
		},
	}.Decl(),
	SystemFunc{
		Id:     "HasSuffix",
		Module: "strings",
		Doc:    "HasSuffix tests whether the string s ends with suffix.",
		Type: types.Func(types.Bool,
			&types.Field{Name: "s", T: types.String},
			&types.Field{Name: "suffix", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			s, suffix := args[0].(string), args[1].(string)
			return strings.HasSuffix(s, suffix), nil
		},
	}.Decl(),
	SystemFunc{
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
	SystemFunc{
		Id:     "FromInt",
		Module: "strings",
		Force:  true,
		Doc:    "FromInt parses an integer into a string.",
		Type: types.Func(types.String,
			&types.Field{Name: "intVal", T: types.Int}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			intVal := args[0].(*big.Int)
			stringVal := intVal.String()
			return stringVal, nil
		},
	}.Decl(),
	SystemFunc{
		Id:     "FromFloat",
		Module: "strings",
		Force:  true,
		Doc:    "FromFloat parses a float into a string with the specified digits of precision.",
		Type: types.Func(types.String,
			&types.Field{Name: "floattVal", T: types.Float},
			&types.Field{Name: "precision", T: types.Int}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			floatVal := args[0].(*big.Float)
			prec := args[1].(*big.Int)
			stringVal := floatVal.Text('g', int(prec.Int64()))
			return stringVal, nil
		},
	}.Decl(),
}

var pathDecls = []*Decl{
	SystemFunc{
		Id:     "Base",
		Module: "path",
		Doc:    "Base returns the last element of path.",
		Type: types.Func(types.String,
			&types.Field{Name: "path", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			return path.Base(args[0].(string)), nil
		},
	}.Decl(),
	SystemFunc{
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
	SystemFunc{
		Id:     "Ext",
		Module: "path",
		Doc:    "Ext returns the file name extension of path.",
		Type: types.Func(types.String,
			&types.Field{Name: "path", T: types.String}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			return path.Ext(args[0].(string)), nil
		},
	}.Decl(),
	SystemFunc{
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

var filesetsDecls = []*Decl{
	SystemFunc{
		Id:     "Dir",
		Module: "filesets",
		Doc:    "Dir returns a fileset from a directory.",
		Type: types.Func(types.Fileset,
			&types.Field{Name: "dir", T: types.Dir}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			if flow, ok := args[0].(*reflow.Flow); ok {
				return coerceFlowToFileset(types.Dir, flow), nil
			}
			return coerceToFileset(types.Dir, args[0]), nil
		},
	}.Decl(),
	SystemFunc{
		Id:     "File",
		Module: "filesets",
		Doc:    "File returns a fileset from a file.",
		Type: types.Func(types.Fileset,
			&types.Field{Name: "file", T: types.File}),
		Do: func(loc values.Location, args []values.T) (values.T, error) {
			if flow, ok := args[0].(*reflow.Flow); ok {
				return coerceFlowToFileset(types.File, flow), nil
			}
			return coerceToFileset(types.File, args[0]), nil
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
		{"filesets", filesetsDecls},
	} {
		lib[mod.name] = &ModuleImpl{Decls: mod.decls}
		lib[mod.name].Init(nil, types.NewEnv())
	}
}
