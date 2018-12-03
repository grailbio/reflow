load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "build.go",
        "unmarshal.go",
    ],
    importmap = "go/src/vendor/github.com/aws/aws-sdk-go/private/protocol/json/jsonutil",
    importpath = "github.com/aws/aws-sdk-go/private/protocol/json/jsonutil",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/private/protocol:go_default_library",
    ],
)
