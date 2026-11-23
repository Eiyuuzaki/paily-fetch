package fetcher

import (
	"context"
	"log"
	"runtime"
	"time"

	"github.com/openpaily/paily-fetch/node"
)

// Config holds all tunable parameters for the Fetcher.
type Config struct {
	// CoreURL is the base URL of paily-core (e.g. "http://localhost:8080").
	CoreURL string
	// ServiceSecret is the Bearer token sent to paily-core.
	ServiceSecret string

	// FetchInterval is the time between fetch rounds.
	FetchInterval time.Duration
	// HTTPConcurrency is the number of parallel HTTP download workers.
	HTTPConcurrency int
	// ParseConcurrency is the number of parallel parse workers.
	// Defaults to runtime.NumCPU() when 0.
	ParseConcurrency int
	// DownloadTimeout is the per-request HTTP timeout.
	DownloadTimeout time.Duration
	// FetchUA is sent with subscription download requests.
	FetchUA string
	// DownloadRetries is the number of extra attempts after the first failure
	// for network-level errors (dial timeout, connection reset, i/o timeout).
	// Defaults to DefaultDownloadRetries when 0. Set to 1 to disable retrying.
	DownloadRetries int
	// RetryBaseDelay is the initial backoff between retries; it doubles each
	// attempt (capped at RetryMaxDelay) with ±25% jitter. Defaults to
	// DefaultRetryBaseDelay when 0.
	RetryBaseDelay time.Duration
	// RetryMaxDelay caps the backoff between retries. Defaults to
	// DefaultRetryMaxDelay when 0.
	RetryMaxDelay time.Duration
	// RoundTimeout bounds a single round. A round that hangs (a stalled DNS
	// lookup inside the container resolver, a worker wedged on a half-open
	// socket) used to eat the whole interval and make the loop look dead: on
	// 2026-11-23 the daemon logged "round done" and then nothing for 67
	// minutes, with no crash and no error. Abandoning the round is the exit
	// condition for that failure mode. Defaults to DefaultRoundTimeout when 0.
	RoundTimeout time.Duration
}

const (
	// DefaultDownloadRetries is the retry count that keeps working on links
	// where a large share of new connections need several SYN retransmissions
	// before the handshake completes. Measured on a home PPPoE line where
	// ~1/3 of connections to overseas CDNs took 5-10s (vs <1s otherwise):
	// a single attempt failed often enough to lose most sources, while
	// 4 retries brought every sample through.
	DefaultDownloadRetries = 4
	// DefaultRetryBaseDelay is the first backoff step.
	DefaultRetryBaseDelay = 1 * time.Second
	// DefaultRetryMaxDelay caps the backoff so a round cannot stall forever.
	DefaultRetryMaxDelay = 8 * time.Second
	// DefaultRoundTimeout is well above a healthy round (seconds) and well
	// below the default 3h interval, so an abandoned round still leaves room
	// for the next tick to start on time.
	DefaultRoundTimeout = 15 * time.Minute
)

func (c *Config) downloadRetries() int {
	if c.DownloadRetries == 0 {
		return DefaultDownloadRetries
	}
	// Negative means "no retries".
	if c.DownloadRetries < 0 {
		return 0
	}
	return c.DownloadRetries
}

func (c *Config) retryBaseDelay() time.Duration {
	if c.RetryBaseDelay > 0 {
		return c.RetryBaseDelay
	}
	return DefaultRetryBaseDelay
}

func (c *Config) retryMaxDelay() time.Duration {
	if c.RetryMaxDelay > 0 {
		return c.RetryMaxDelay
	}
	return DefaultRetryMaxDelay
}

func (c *Config) parseConcurrency() int {
	if c.ParseConcurrency > 0 {
		return c.ParseConcurrency
	}
	return runtime.NumCPU()
}

func (c *Config) roundTimeout() time.Duration {
	if c.RoundTimeout > 0 {
		return c.RoundTimeout
	}
	return DefaultRoundTimeout
}

func (c *Config) fetchUA() string {
	if c.FetchUA != "" {
		return c.FetchUA
	}
	return defaultFetchUA
}

// Fetcher periodically pulls sources from paily-core, processes nodes, and reports back.
type Fetcher struct {
	cfg    Config
	client *APIClient
}

// New creates a new Fetcher.
func New(cfg Config) *Fetcher {
	return &Fetcher{
		cfg:    cfg,
		client: NewAPIClient(cfg.CoreURL, cfg.ServiceSecret),
	}
}

// Run starts the fetch loop and blocks until ctx is cancelled.
func (f *Fetcher) Run(ctx context.Context) {
	log.Printf("[fetcher] starting — interval %s, http-workers %d, parse-workers %d",
		f.cfg.FetchInterval, f.cfg.HTTPConcurrency, f.cfg.parseConcurrency())

	// Run once immediately, then on each tick.
	f.runRound(ctx)

	ticker := time.NewTicker(f.cfg.FetchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("[fetcher] context cancelled — stopping")
			return
		case <-ticker.C:
			f.runRound(ctx)
		}
	}
}

// runRound wraps runOnce in a deadline and always logs the outcome, so a
// stalled round is visible in the log instead of silently swallowing the
// interval. The goroutine is cancelled through roundCtx rather than leaked:
// every HTTP call in the pipeline is ctx-aware.
func (f *Fetcher) runRound(ctx context.Context) {
	timeout := f.cfg.roundTimeout()
	roundCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.runOnce(roundCtx)
	}()

	select {
	case <-done:
		log.Printf("[fetcher] round clock — elapsed %s", time.Since(start).Round(time.Millisecond))
	case <-roundCtx.Done():
		if ctx.Err() != nil {
			log.Println("[fetcher] context cancelled during round — stopping")
			return
		}
		log.Printf("[fetcher] round exceeded %s — abandoning it, next tick starts clean", timeout)
	}
}

// runOnce executes a single fetch round.
func (f *Fetcher) runOnce(ctx context.Context) {
	log.Println("[fetcher] round start")

	sources, err := f.client.GetSources(ctx)
	if err != nil {
		log.Printf("[fetcher] get sources failed: %v — will retry next interval", err)
		return
	}
	log.Printf("[fetcher] %d sources", len(sources))

	if len(sources) == 0 {
		return
	}

	results := f.processSources(ctx, sources)

	if err := f.client.PostResults(ctx, results); err != nil {
		log.Printf("[fetcher] post results failed: %v", err)
		return
	}

	var nodesTotal, okSources, failedSources, emptySources int
	for _, r := range results {
		if r.Success && r.NodeCount != nil {
			nodesTotal += *r.NodeCount
			if *r.NodeCount > 0 {
				okSources++
			} else {
				emptySources++
			}
		} else {
			failedSources++
		}
	}
	// ok + empty + failed always equals the posted count: an empty source used
	// to be indistinguishable from a healthy one here, which is how a source
	// feeding zero nodes for a whole day went unnoticed.
	log.Printf("[fetcher] round done — posted %d results, total nodes %d (ok=%d empty=%d failed=%d)",
		len(results), nodesTotal, okSources, emptySources, failedSources)
}

// processSources runs a fully pipelined download → parse → collect flow.
//
// Pipeline stages (all run concurrently):
//
//	Stage 1 — HTTP workers:   download subscribe sources in parallel → dlCh
//	Stage 2 — Bridge:         node sources → parseJobsCh immediately;
//	                          subscribe results from dlCh → parseJobsCh (success)
//	                          or failCh (download failure)
//	Stage 3 — Parse workers:  parse subscription bodies in parallel → parsedCh
//	Stage 4 — Collect:        drain parsedCh then failCh into []FetchResult
//
// Parse workers start before any download completes so that CPU capacity is
// utilised as soon as the first response arrives.
func (f *Fetcher) processSources(ctx context.Context, sources []Source) []FetchResult {
	// ── Split by type ─────────────────────────────────────────────────────────
	var subscribeSources, nodeSources []Source
	for _, s := range sources {
		if s.Type == "subscribe" {
			subscribeSources = append(subscribeSources, s)
		} else {
			nodeSources = append(nodeSources, s)
		}
	}

	// ── Stage 1: HTTP workers (start immediately) ─────────────────────────────
	jobsCh := make(chan downloadJob, len(subscribeSources))
	for _, s := range subscribeSources {
		jobsCh <- downloadJob{source: s}
	}
	close(jobsCh)
	dlCh := runHTTPWorkers(ctx, jobsCh, f.cfg.HTTPConcurrency, f.cfg.DownloadTimeout, f.cfg.fetchUA(),
		retryConfig{
			retries:   f.cfg.downloadRetries(),
			baseDelay: f.cfg.retryBaseDelay(),
			maxDelay:  f.cfg.retryMaxDelay(),
		})

	// ── Stage 3: Parse workers (start immediately, before any download done) ──
	// Small buffer: parse workers pull jobs the moment they arrive.
	parseJobsCh := make(chan processJob, f.cfg.parseConcurrency()*2)
	parsedCh := runParseWorkers(ctx, parseJobsCh, f.cfg.parseConcurrency())

	// ── Stage 2: Bridge goroutine ─────────────────────────────────────────────
	// Buffered so the bridge never blocks even if collector hasn't reached failCh yet.
	failCh := make(chan FetchResult, len(subscribeSources)+1)
	go func() {
		defer close(parseJobsCh)
		defer close(failCh)

		// Node-type sources need no download — feed parse workers right away.
		for _, s := range nodeSources {
			select {
			case parseJobsCh <- processJob{source: s}:
			case <-ctx.Done():
				return
			}
		}
		// Stream download results into parse workers as they arrive.
		for {
			var dr downloadResult
			var ok bool
			select {
			case dr, ok = <-dlCh:
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}
			if len(dr.bodies) == 0 {
				log.Printf("[fetcher] source %s (%s): all fetches failed: %v", dr.source.ID, dr.source.Identifier, dr.err)
				select {
				case failCh <- FetchResult{SourceID: dr.source.ID, Success: false}:
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case parseJobsCh <- processJob{source: dr.source, bodies: dr.bodies, userinfo: dr.userinfo}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// ── Stage 4: Collect ──────────────────────────────────────────────────────
	results := make([]FetchResult, 0, len(sources))

	// Drain parse results first (keeps the bridge goroutine and parse workers busy).
	for pr := range parsedCh {
		if pr.err != nil && len(pr.nodes) == 0 {
			results = append(results, FetchResult{
				SourceID: pr.source.ID,
				Success:  false,
			})
			continue
		}
		clashMaps := nodesToClashMaps(pr.nodes)
		count := len(clashMaps)
		results = append(results, FetchResult{
			SourceID:  pr.source.ID,
			Success:   true,
			NodeCount: &count,
			Nodes:     clashMaps,
			UsageInfo: parseUserinfo(pr.userinfo),
		})
	}

	// Drain download failures (already fully buffered by the time parsedCh closes).
	for fr := range failCh {
		results = append(results, fr)
	}

	return results
}

// nodesToClashMaps extracts the raw clash maps from a list of ProxyNodes.
func nodesToClashMaps(nodes []*node.ProxyNode) []map[string]any {
	out := make([]map[string]any, len(nodes))
	for i, n := range nodes {
		out[i] = n.ClashMap()
	}
	return out
}
