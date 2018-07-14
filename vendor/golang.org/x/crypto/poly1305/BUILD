load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "const_amd64.s",
        "poly1305.go",
        "poly1305_amd64.s",
        "poly1305_arm.s",
        "sum_amd64.go",
        "sum_arm.go",
    ],
    importmap = "grail/go/src/vendor/golang.org/x/crypto/poly1305",
    importpath = "golang.org/x/crypto/poly1305",
    visibility = ["//visibility:public"],
)
