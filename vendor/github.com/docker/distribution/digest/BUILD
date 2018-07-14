load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "digest.go",
        "digester.go",
        "doc.go",
        "set.go",
        "verifiers.go",
    ],
    importmap = "grail/go/src/vendor/github.com/docker/distribution/digest",
    importpath = "github.com/docker/distribution/digest",
    visibility = ["//visibility:public"],
)
