load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["interface.go"],
    importmap = "go/src/vendor/github.com/aws/aws-sdk-go/service/ecr/ecriface",
    importpath = "github.com/aws/aws-sdk-go/service/ecr/ecriface",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/request:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/service/ecr:go_default_library",
    ],
)
