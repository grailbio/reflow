// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package staticconfig

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/pool/client"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/net/http2"
)

func init() {
	config.Register(config.Cluster, "static", "", "configure a static cluster",
		func(cfg config.Config, arg string) (config.Config, error) {
			if arg == "" {
				return nil, errors.New("hosts not provided")
			}
			return &cluster{cfg, strings.Split(arg, ",")}, nil
		},
	)
}

type cluster struct {
	config.Config
	hosts []string
}

func (c *cluster) Cluster() (runner.Cluster, error) {
	clientConfig, _, err := c.HTTPS()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	http2.ConfigureTransport(transport)
	httpClient := &http.Client{Transport: transport}

	pools := make([]pool.Pool, len(c.hosts))
	for i, host := range c.hosts {
		url := fmt.Sprintf("https://%s/v1/", host)
		var err error
		pools[i], err = client.New(url, httpClient, nil)
		if err != nil {
			return nil, err
		}
	}
	mux := new(pool.Mux)
	mux.SetPools(pools)
	return &runner.StaticCluster{mux}, nil
}
