load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "attributes.go",
        "gotty.go",
        "parser.go",
        "types.go",
    ],
    importmap = "grail/go/src/vendor/github.com/Nvveen/Gotty",
    importpath = "github.com/Nvveen/Gotty",
    visibility = ["//visibility:public"],
)
