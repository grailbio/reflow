load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["bloom.go"],
    importmap = "grail/go/src/vendor/github.com/willf/bloom",
    importpath = "github.com/willf/bloom",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/spaolacci/murmur3:go_default_library",
        "//go/src/vendor/github.com/willf/bitset:go_default_library",
    ],
)
