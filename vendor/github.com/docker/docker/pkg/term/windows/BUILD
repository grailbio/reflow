load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "ansi_reader.go",
        "ansi_writer.go",
        "console.go",
        "windows.go",
    ],
    importmap = "grail/go/src/vendor/github.com/docker/docker/pkg/term/windows",
    importpath = "github.com/docker/docker/pkg/term/windows",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/Azure/go-ansiterm:go_default_library",
        "//go/src/vendor/github.com/Sirupsen/logrus:go_default_library",
    ] + select({
        "@io_bazel_rules_go//go/platform:windows": [
            "//go/src/vendor/github.com/Azure/go-ansiterm/winterm:go_default_library",
        ],
        "//conditions:default": [],
    }),
)
