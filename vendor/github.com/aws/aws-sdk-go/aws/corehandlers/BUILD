load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "handlers.go",
        "param_validator.go",
        "user_agent.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-sdk-go/aws/corehandlers",
    importpath = "github.com/aws/aws-sdk-go/aws/corehandlers",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/awserr:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/credentials:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/request:go_default_library",
    ],
)
