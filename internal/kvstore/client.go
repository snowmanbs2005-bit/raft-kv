package kvstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a small HTTP client for the KV API, used by raftctl. It is
// handed a list of known peer HTTP addresses (any subset of the cluster is
// enough) and: tries them in order, follows a single leader redirect when a
// non-leader node responds with 307, and retries against the next known
// peer if a request fails outright (e.g. that node is down or mid-election
// with no leader yet).
type Client struct {
	peerAddrs  []string
	httpClient *http.Client
}

// NewClient returns a Client that will try each of peerAddrs (host:port,
// no scheme) in order.
func NewClient(peerAddrs []string) *Client {
	return &Client{
		peerAddrs: peerAddrs,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // we follow redirects manually
			},
		},
	}
}

// Put sets key to value.
func (c *Client) Put(key, value string) error {
	_, err := c.doWithRetry(http.MethodPut, key, []byte(value))
	return err
}

// Get retrieves key's value.
func (c *Client) Get(key string) (string, error) {
	body, err := c.doWithRetry(http.MethodGet, key, nil)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Delete removes key.
func (c *Client) Delete(key string) error {
	_, err := c.doWithRetry(http.MethodDelete, key, nil)
	return err
}

// Leader asks each known peer for its status and returns the HTTP address
// of whichever peer reports itself as leader (or is aware of one). It
// checks the peer list in order and returns on the first useful answer.
func (c *Client) Leader() (addr string, err error) {
	var lastErr error
	for _, peer := range c.peerAddrs {
		resp, err := c.httpClient.Get("http://" + peer + "/status")
		if err != nil {
			lastErr = err
			continue
		}
		var status StatusResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()
		if decodeErr != nil {
			lastErr = decodeErr
			continue
		}
		if status.IsLeader {
			return peer, nil
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("kvstore client: could not determine leader: %w", lastErr)
	}
	return "", fmt.Errorf("kvstore client: no peer currently reports being leader")
}

func (c *Client) doWithRetry(method, key string, body []byte) ([]byte, error) {
	if len(c.peerAddrs) == 0 {
		return nil, fmt.Errorf("kvstore client: no peer addresses configured")
	}

	var lastErr error
	for _, addr := range c.peerAddrs {
		out, err := c.doOnce(method, addr, key, body)
		if err == nil {
			return out, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("kvstore client: all peers failed, last error: %w", lastErr)
}

func (c *Client) doOnce(method, addr, key string, body []byte) ([]byte, error) {
	url := "http://" + addr + "/kv/" + key
	resp, err := c.request(method, url, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
		loc := resp.Header.Get("Location")
		if loc == "" {
			return nil, fmt.Errorf("redirect with no Location header")
		}
		resp2, err := c.request(method, loc, body)
		if err != nil {
			return nil, err
		}
		defer resp2.Body.Close()
		return readResult(resp2)
	}

	return readResult(resp)
}

func (c *Client) request(method, url string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}

func readResult(resp *http.Response) ([]byte, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(data))
	}
	return data, nil
}
