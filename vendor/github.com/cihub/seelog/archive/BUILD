load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["archive.go"],
    importmap = "grail/go/src/vendor/github.com/cihub/seelog/archive",
    importpath = "github.com/cihub/seelog/archive",
    visibility = ["//visibility:public"],
    deps = ["//go/src/vendor/github.com/cihub/seelog/archive/gzip:go_default_library"],
)
