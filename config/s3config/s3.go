// Package s3config defines a configuration provider named "s3"
// which can be used to configure S3-based caches.
package s3config

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/cache"
	"github.com/grailbio/reflow/cache/dynamodbassoc"
	"github.com/grailbio/reflow/config"
	reflows3 "github.com/grailbio/reflow/repository/s3"
	"grail.com/lib/limiter"
)

func init() {
	config.Register(config.Cache, "s3", "bucket,table", "configure a cache using an S3 bucket and DynamoDB table",
		func(cfg config.Config, arg string) (config.Config, error) {
			parts := strings.Split(arg, ",")
			if n := len(parts); n != 2 {
				return nil, fmt.Errorf("cache: s3: expected 2 arguments, got %d", n)
			}
			cache := &Cache{Config: cfg, Bucket: parts[0], Table: parts[1]}
			return cache, nil
		},
	)
}

// Cache is a cache configuration based on S3 (for objects) and
// DynamoDB (for assocs).
type Cache struct {
	config.Config
	Bucket, Table string
}

// Cache returns a new cache instance as configured by this cache
// configuration.
func (c *Cache) Cache() (reflow.Cache, error) {
	sess, err := c.AWS()
	if err != nil {
		return nil, err
	}
	repo := reflows3.Repository{
		Bucket: c.Bucket,
		Client: s3.New(sess),
	}
	assoc := &dynamodbassoc.Assoc{
		DB:        dynamodb.New(sess),
		TableName: c.Table,
	}
	cache := &cache.Cache{
		Repository: &repo,
		Assoc:      assoc,
		WriteLim:   limiter.New(),
		LookupLim:  limiter.New(),
	}
	// TODO(marius): make this configurable.
	const concurrency = 512
	cache.WriteLim.Release(concurrency)
	cache.LookupLim.Release(concurrency)
	return cache, nil
}
