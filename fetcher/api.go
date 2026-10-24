package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// APIClient communicates with paily-core over HTTP.
type APIClient struct {
	baseURL string
	secret  string
	hc      *http.Client
}

// NewAPIClient creates a client pointing at baseURL authenticated with secret.
func NewAPIClient(baseURL, secret string) *APIClient {
	return &APIClient{
		baseURL: baseURL,
		secret:  secret,
		hc: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ─── API types ───────────────────────────────────────────────────────────────

// Source mirrors the API's SourceDetail object.
type Source struct {
	ID         string `json:"id"`
	Type       string `json:"type"`    // "subscribe" | "node"
	Identifier string `json:"identifier"`
	Info       string `json:"info"`
	Status     string `json:"status"`
	Content    string `json:"content"` // URL or raw content
}

type fetchSourcesResponse struct {
	Sources []Source `json:"sources"`
}

// FetchResult is a single entry in the POST /api/v1/fetch/results payload.
type FetchResult struct {
	SourceID  string           `json:"source_id"`
	Success   bool             `json:"success"`
	NodeCount *int             `json:"node_count,omitempty"`
	Nodes     []map[string]any `json:"nodes,omitempty"`
	UsageInfo *UsageInfo       `json:"usage_info,omitempty"`
}

// UsageInfo mirrors the subscription-userinfo header of the source's
// subscription response. Nil when the source sent no such header (free
// GitHub / TG sources) — the core API treats nil as "keep previous value".
type UsageInfo struct {
	Upload   int64 `json:"upload"`
	Download int64 `json:"download"`
	Total    int64 `json:"total"`
	Expire   int64 `json:"expire"`
}

// userinfoKeyRe matches key=value pairs inside a subscription-userinfo
// header value, e.g. "upload=1; download=2; total=3; expire=1735689600".
var userinfoKeyRe = regexp.MustCompile(`(?i)(upload|download|total|expire)\s*=\s*(\d+)`)

// parseUserinfo converts a subscription-userinfo header value into UsageInfo.
// Returns nil when the header is empty or no recognisable keys are present,
// which the core API interprets as "no quota information available".
func parseUserinfo(header string) *UsageInfo {
	if header == "" {
		return nil
	}
	ui := &UsageInfo{}
	found := false
	for _, m := range userinfoKeyRe.FindAllStringSubmatch(header, -1) {
		v, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			continue
		}
		switch strings.ToLower(m[1]) {
		case "upload":
			ui.Upload = v
		case "download":
			ui.Download = v
		case "total":
			ui.Total = v
		case "expire":
			ui.Expire = v
		}
		found = true
	}
	if !found {
		return nil
	}
	return ui
}

type fetchResultsRequest struct {
	Results []FetchResult `json:"results"`
}

// ─── Methods ─────────────────────────────────────────────────────────────────

// GetSources fetches the list of active sources from the API.
func (c *APIClient) GetSources(ctx context.Context) ([]Source, error) {
	body, err := c.get(ctx, "/api/v1/fetch/sources")
	if err != nil {
		return nil, err
	}
	var resp fetchSourcesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode sources response: %w", err)
	}
	return resp.Sources, nil
}

// PostResults submits fetch results to the API.
func (c *APIClient) PostResults(ctx context.Context, results []FetchResult) error {
	payload := fetchResultsRequest{Results: results}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	return c.post(ctx, "/api/v1/fetch/results", data)
}

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

func (c *APIClient) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return body, nil
}

func (c *APIClient) post(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d", path, resp.StatusCode)
	}
	return nil
}
