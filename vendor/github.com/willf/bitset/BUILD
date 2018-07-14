load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "bitset.go",
        "popcnt.go",
        "popcnt_amd64.go",
        "popcnt_amd64.s",
    ],
    importmap = "grail/go/src/vendor/github.com/willf/bitset",
    importpath = "github.com/willf/bitset",
    visibility = ["//visibility:public"],
)
