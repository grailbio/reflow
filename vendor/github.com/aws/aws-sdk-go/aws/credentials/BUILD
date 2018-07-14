load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "chain_provider.go",
        "credentials.go",
        "env_provider.go",
        "shared_credentials_provider.go",
        "static_provider.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-sdk-go/aws/credentials",
    importpath = "github.com/aws/aws-sdk-go/aws/credentials",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/awserr:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/internal/shareddefaults:go_default_library",
        "//go/src/vendor/github.com/go-ini/ini:go_default_library",
    ],
)
