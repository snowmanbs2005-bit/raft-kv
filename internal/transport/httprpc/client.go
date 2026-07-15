package httprpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
)

// Client implements raft.Transport over HTTP. It keeps one shared
// *http.Client (and therefore one pooled, keep-alive connection per peer)
// for the lifetime of the node, playing the same role that a pooled
// grpc.ClientConn would in a gRPC-based transport.
type Client struct {
	addrOf     func(peerID string) (string, bool)
	httpClient *http.Client
}

// NewClient returns a Client that resolves peer IDs to "host:port" raft
// addresses via addrOf.
func NewClient(addrOf func(peerID string) (string, bool)) *Client {
	return &Client{
		addrOf: addrOf,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// RequestVote implements raft.Transport.
func (c *Client) RequestVote(ctx context.Context, peerID string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := c.call(ctx, peerID, requestVotePath, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// AppendEntries implements raft.Transport.
func (c *Client) AppendEntries(ctx context.Context, peerID string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := c.call(ctx, peerID, appendEntriesPath, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *Client) call(ctx context.Context, peerID, path string, args, reply any) error {
	addr, ok := c.addrOf(peerID)
	if !ok {
		return fmt.Errorf("httprpc: unknown peer %q", peerID)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("httprpc: marshal request: %w", err)
	}
	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("httprpc: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("httprpc: request to %s: %w", peerID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("httprpc: peer %s returned status %d", peerID, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(reply); err != nil {
		return fmt.Errorf("httprpc: decode reply from %s: %w", peerID, err)
	}
	return nil
}
