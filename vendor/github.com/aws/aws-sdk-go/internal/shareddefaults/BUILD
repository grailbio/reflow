load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["shared_config.go"],
    importmap = "grail/go/src/vendor/github.com/aws/aws-sdk-go/internal/shareddefaults",
    importpath = "github.com/aws/aws-sdk-go/internal/shareddefaults",
    visibility = ["//go/src/vendor/github.com/aws/aws-sdk-go:__subpackages__"],
)
