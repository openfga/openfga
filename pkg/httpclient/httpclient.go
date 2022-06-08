package httpclient

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type RetryableHTTPClient struct {
	internalClient http.Client
}

func NewRetryableHTTPClient() *RetryableHTTPClient {
	return &RetryableHTTPClient{
		internalClient: http.Client{},
	}
}

func (client RetryableHTTPClient) ExecuteRequest(req *http.Request) (*http.Response, []byte, error) {
	var body []byte
	var err error
	var resp *http.Response

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 3 * time.Second

	err = backoff.Retry(
		func() error {
			resp, body, err = client.executeRequest(req)
			if err != nil {
				return err
			}

			return nil
		},
		backoffPolicy,
	)

	// All retries failed
	if err != nil {
		return nil, nil, err
	}

	return resp, body, nil
}

func (client RetryableHTTPClient) executeRequest(req *http.Request) (*http.Response, []byte, error) {
	resp, err := client.internalClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, body, nil
}
