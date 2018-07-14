load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "default_exception_formatting_strategy.go",
        "exception_formatting_strategy.go",
        "xray_error.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-xray-sdk-go/strategy/exception",
    importpath = "github.com/aws/aws-xray-sdk-go/strategy/exception",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/awserr:go_default_library",
        "//go/src/vendor/github.com/pkg/errors:go_default_library",
    ],
)
