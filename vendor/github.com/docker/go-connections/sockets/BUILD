load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "inmem_socket.go",
        "proxy.go",
        "sockets.go",
        "sockets_unix.go",
        "sockets_windows.go",
        "tcp_socket.go",
        "unix_socket.go",
    ],
    importmap = "grail/go/src/vendor/github.com/docker/go-connections/sockets",
    importpath = "github.com/docker/go-connections/sockets",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/golang.org/x/net/proxy:go_default_library",
    ] + select({
        "@io_bazel_rules_go//go/platform:freebsd": [
            "//go/src/vendor/github.com/Sirupsen/logrus:go_default_library",
            "//go/src/vendor/github.com/opencontainers/runc/libcontainer/user:go_default_library",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//go/src/vendor/github.com/Sirupsen/logrus:go_default_library",
            "//go/src/vendor/github.com/opencontainers/runc/libcontainer/user:go_default_library",
        ],
        "@io_bazel_rules_go//go/platform:solaris": [
            "//go/src/vendor/github.com/Sirupsen/logrus:go_default_library",
            "//go/src/vendor/github.com/opencontainers/runc/libcontainer/user:go_default_library",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//go/src/vendor/github.com/Microsoft/go-winio:go_default_library",
        ],
        "//conditions:default": [],
    }),
)
