package repository

import "flag"

type RepoFlagsTrait struct {
	BucketName string
}

func (r *RepoFlagsTrait) Flags(flags *flag.FlagSet) {
	flags.StringVar(&r.BucketName, "bucket", "", "bucket name")
}
