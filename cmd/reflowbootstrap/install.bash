#!/bin/bash
#
# install.bash is a script to build a reflow bootstrap binary and push to a public S3 bucket.
# To build and release a new version, update VERSION below and run the script.
# Make sure to update the latest path in: go/src/github.com/grailbio/reflow/cmd/reflow/main.go
#
# Reflow uses this binary as a bootstrap on reflowlet instances.
#
set -e
set -x

VERSION=reflowbootstrap0.2

GOOS=linux GOARCH=amd64 go build -o /tmp/$VERSION .
cloudkey ti-apps/admin aws s3 cp --acl public-read /tmp/$VERSION s3://grail-public-bin/linux/amd64/$VERSION

