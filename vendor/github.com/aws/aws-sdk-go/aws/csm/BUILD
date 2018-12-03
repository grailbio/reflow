load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "enable.go",
        "metric.go",
        "metric_chan.go",
        "reporter.go",
    ],
    importmap = "go/src/vendor/github.com/aws/aws-sdk-go/aws/csm",
    importpath = "github.com/aws/aws-sdk-go/aws/csm",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/awserr:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/request:go_default_library",
    ],
)
