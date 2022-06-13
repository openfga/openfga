package retryablehttp

import (
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
)

const backOffMaxDuration = 3 * time.Second

type RetryableRoundTripper struct {
	Client *RetryableHTTPClient
	once   sync.Once
}

// RoundTripper is an interface representing the ability to execute a single HTTP transaction, obtaining the Response for a given Request.
// A RoundTripper must be safe for concurrent use by multiple goroutines.
var _ http.RoundTripper = (*RetryableRoundTripper)(nil)

func (rt *RetryableRoundTripper) init() {
	if rt.Client == nil {
		rt.Client = NewClient()
	}
}

// RoundTrip executes a single HTTP transaction, but does not attempt to read the response, modify it, or close the body.
func (rt *RetryableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.once.Do(rt.init)
	return rt.Client.Do(req)
}

type RetryableHTTPClient struct {
	internalClient http.Client
}

func NewClient() *RetryableHTTPClient {
	return &RetryableHTTPClient{
		internalClient: http.Client{},
	}
}

func (client *RetryableHTTPClient) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (client *RetryableHTTPClient) Do(req *http.Request) (*http.Response, error) {
	var err error
	var resp *http.Response

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = backOffMaxDuration

	err = backoff.Retry(
		func() error {
			resp, err = client.internalClient.Do(req)
			if err != nil {
				return err
			}

			return nil
		},
		backoffPolicy,
	)

	// All retries failed
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (client *RetryableHTTPClient) StandardClient() *http.Client {
	return &http.Client{
		Transport: &RetryableRoundTripper{Client: client},
	}
}
