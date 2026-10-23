package fetcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newRetry builds a retryConfig with zero backoff so tests run fast.
func newRetry(retries int) retryConfig {
	return retryConfig{retries: retries, baseDelay: 0, maxDelay: 0}
}

// TestDownloadURL_RetriesTransientErrors verifies a transport failure is
// retried and eventually succeeds. This is the behaviour that rescues sources
// on a line where most new connections need several SYN retransmissions.
func TestDownloadURL_RetriesTransientErrors(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			// Simulate a dead connection like the PPPoE line produces.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("cannot hijack")
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	body, err := downloadURLWithRetry(context.Background(), srv.Client(),
		"ua", srv.URL, newRetry(4))
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

// TestDownloadURL_NoRetryOnHTTPStatus verifies 404/403 are NOT retried:
// retrying cannot fix them and would only stall the round.
func TestDownloadURL_NoRetryOnHTTPStatus(t *testing.T) {
	for _, code := range []int{404, 403, 500} {
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(code)
		}))

		_, err := downloadURLWithRetry(context.Background(), srv.Client(),
			"ua", srv.URL, newRetry(4))
		if err == nil {
			t.Fatalf("code %d: expected error", code)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", code)) {
			t.Fatalf("code %d: err = %v", code, err)
		}
		if got := atomic.LoadInt32(&calls); got != 1 {
			t.Fatalf("code %d: calls = %d, want 1 (must not retry)", code, got)
		}
		srv.Close()
	}
}

// TestDownloadURL_ExhaustsRetries verifies the final error is returned when
// every attempt fails.
func TestDownloadURL_ExhaustsRetries(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	_, err := downloadURLWithRetry(context.Background(), srv.Client(),
		"ua", srv.URL, newRetry(3))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Fatalf("calls = %d, want 4 (1 + 3 retries)", got)
	}
}

// TestDownloadURL_ZeroRetries verifies retries=0 behaves like a single attempt.
func TestDownloadURL_ZeroRetries(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	_, err := downloadURLWithRetry(context.Background(), srv.Client(),
		"ua", srv.URL, newRetry(0))
	if err == nil {
		t.Fatal("expected error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

// TestIsRetryable classifies errors correctly.
func TestIsRetryable(t *testing.T) {
	retryable := []error{
		errors.New(`Get "https://x": dial tcp 1.2.3.4:443: i/o timeout`),
		errors.New("context deadline exceeded (Client.Timeout exceeded while awaiting headers)"),
		errors.New("read tcp: connection reset by peer"),
		errors.New("dial tcp: lookup x on 127.0.0.11:53: read udp: i/o timeout"),
		errors.New("http: TLS handshake timeout"),
	}
	for _, e := range retryable {
		if !isRetryable(e) {
			t.Errorf("expected retryable: %v", e)
		}
	}

	notRetryable := []error{
		errors.New("HTTP 404"),
		errors.New("HTTP 403 Forbidden"),
		fmt.Errorf("build request: %w", errors.New("unsupported protocol scheme")),
	}
	for _, e := range notRetryable {
		if isRetryable(e) {
			t.Errorf("expected NOT retryable: %v", e)
		}
	}
}

// TestBackoffFor verifies exponential growth, the cap, and bounds.
func TestBackoffFor(t *testing.T) {
	r := retryConfig{retries: 5, baseDelay: time.Second, maxDelay: 8 * time.Second}
	for n := 1; n <= 6; n++ {
		d := r.backoffFor(n)
		// ±25% jitter around the nominal value.
		nominal := time.Second
		for i := 1; i < n; i++ {
			nominal *= 2
			if nominal >= r.maxDelay {
				nominal = r.maxDelay
				break
			}
		}
		if nominal > r.maxDelay {
			nominal = r.maxDelay
		}
		lo, hi := nominal*3/4, nominal*5/4
		if d < lo || d > hi {
			t.Errorf("backoffFor(%d) = %v, want within [%v, %v]", n, d, lo, hi)
		}
		if d < 0 {
			t.Errorf("backoffFor(%d) negative: %v", n, d)
		}
	}
}

// TestConfigDefaults verifies the documented defaults and the "off" switches.
func TestConfigDefaults(t *testing.T) {
	c := Config{}
	if got := c.downloadRetries(); got != DefaultDownloadRetries {
		t.Errorf("default retries = %d, want %d", got, DefaultDownloadRetries)
	}
	if got := c.retryBaseDelay(); got != DefaultRetryBaseDelay {
		t.Errorf("default base delay = %v", got)
	}
	if got := c.retryMaxDelay(); got != DefaultRetryMaxDelay {
		t.Errorf("default max delay = %v", got)
	}

	// Explicit values must win.
	c2 := Config{DownloadRetries: 7, RetryBaseDelay: 2 * time.Second, RetryMaxDelay: 20 * time.Second}
	if got := c2.downloadRetries(); got != 7 {
		t.Errorf("explicit retries = %d, want 7", got)
	}

	// Negative disables.
	c3 := Config{DownloadRetries: -1}
	if got := c3.downloadRetries(); got != 0 {
		t.Errorf("retries with -1 = %d, want 0", got)
	}
}

// TestDownloadURL_ContextCancelled verifies a cancelled context aborts the
// retry loop instead of sleeping through the whole backoff schedule.
func TestDownloadURL_ContextCancelled(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Large backoff so the test would time out if the loop ignored ctx.
	r := retryConfig{retries: 5, baseDelay: 10 * time.Second, maxDelay: 10 * time.Second}
	start := time.Now()
	_, err := downloadURLWithRetry(ctx, srv.Client(), "ua", srv.URL, r)
	if err == nil {
		t.Fatal("expected error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v — context cancellation not honoured", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
