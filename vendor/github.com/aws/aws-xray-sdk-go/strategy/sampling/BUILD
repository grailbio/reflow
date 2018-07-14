load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "localized.go",
        "reservoir.go",
        "sampling_rule.go",
        "sampling_rule_manifest.go",
        "sampling_strategy.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-xray-sdk-go/strategy/sampling",
    importpath = "github.com/aws/aws-xray-sdk-go/strategy/sampling",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-xray-sdk-go/pattern:go_default_library",
        "//go/src/vendor/github.com/aws/aws-xray-sdk-go/resources:go_default_library",
        "//go/src/vendor/github.com/cihub/seelog:go_default_library",
    ],
)
