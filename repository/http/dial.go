package http

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/rest"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/client"
)

func init() {
	repository.RegisterScheme("https", Dial)
}

// HTTPClient is the client that is used to instantiate the (REST)
// API client for remote repositories.
var HTTPClient *http.Client

var (
	mu sync.Mutex
	// clients caches repository clients.
	clients = map[string]*client.Client{}
)

// Dial implements repository dialling for https urls.
func Dial(u *url.URL) (reflow.Repository, error) {
	if u.Scheme != "https" {
		return nil, errors.E("dial", u.String(), errors.NotSupported, errors.Errorf("unknown scheme %q", u.Scheme))
	}
	mu.Lock()
	defer mu.Unlock()
	key := u.String()
	if clients[key] == nil {
		clients[key] = &client.Client{rest.NewClient(HTTPClient, u, nil)}
	}
	return clients[key], nil
}
