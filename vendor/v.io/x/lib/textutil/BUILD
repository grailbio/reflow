load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "doc.go",
        "rune.go",
        "utf8.go",
        "util_unix.go",
        "util_windows.go",
        "wrap_writer.go",
        "writer.go",
    ],
    importmap = "grail/go/src/vendor/v.io/x/lib/textutil",
    importpath = "v.io/x/lib/textutil",
    visibility = ["//visibility:public"],
)
