load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["parse.go"],
    importmap = "grail/go/src/vendor/github.com/docker/docker/api/types/filters",
    importpath = "github.com/docker/docker/api/types/filters",
    visibility = ["//visibility:public"],
    deps = ["//go/src/vendor/github.com/docker/docker/api/types/versions:go_default_library"],
)
