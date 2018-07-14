load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "ascii.go",
        "tc_linux_cgo.go",
        "tc_other.go",
        "tc_solaris_cgo.go",
        "term.go",
        "term_solaris.go",
        "term_unix.go",
        "term_windows.go",
        "termios_darwin.go",
        "termios_freebsd.go",
        "termios_linux.go",
        "termios_openbsd.go",
    ],
    cgo = True,
    importmap = "grail/go/src/vendor/github.com/docker/docker/pkg/term",
    importpath = "github.com/docker/docker/pkg/term",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:windows": [
            "//go/src/vendor/github.com/Azure/go-ansiterm/winterm:go_default_library",
            "//go/src/vendor/github.com/docker/docker/pkg/term/windows:go_default_library",
        ],
        "//conditions:default": [],
    }),
)
