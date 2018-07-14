load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "converter.go",
        "decode.go",
        "doc.go",
        "encode.go",
        "field.go",
        "tag.go",
    ],
    importmap = "grail/go/src/vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute",
    importpath = "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute",
    visibility = ["//visibility:public"],
    deps = [
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/aws/awserr:go_default_library",
        "//go/src/vendor/github.com/aws/aws-sdk-go/service/dynamodb:go_default_library",
    ],
)
