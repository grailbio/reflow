load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "context_missing.go",
        "default_context_missing.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-xray-sdk-go/strategy/ctxmissing",
    importpath = "github.com/aws/aws-xray-sdk-go/strategy/ctxmissing",
    visibility = ["//visibility:public"],
    deps = ["//go/src/vendor/github.com/cihub/seelog:go_default_library"],
)
