load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["plugin.go"],
    importmap = "grail/go/src/vendor/github.com/aws/aws-xray-sdk-go/internal/plugins",
    importpath = "github.com/aws/aws-xray-sdk-go/internal/plugins",
    visibility = ["//go/src/vendor/github.com/aws/aws-xray-sdk-go:__subpackages__"],
)
