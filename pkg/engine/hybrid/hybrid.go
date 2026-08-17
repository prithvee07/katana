package hybrid

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/cdp"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/launcher/flags"
	"github.com/go-rod/rod/lib/proto"
	"github.com/projectdiscovery/gologger"
	"github.com/projectdiscovery/katana/pkg/engine/common"
	"github.com/projectdiscovery/katana/pkg/navigation"
	"github.com/projectdiscovery/katana/pkg/output"
	"github.com/projectdiscovery/katana/pkg/types"
	"github.com/projectdiscovery/katana/pkg/utils"
	"github.com/projectdiscovery/utils/errkit"
	urlutil "github.com/projectdiscovery/utils/url"
	"github.com/remeh/sizedwaitgroup"
)

// browserAgent is one isolated Chrome instance used as a hybrid crawl worker.
type browserAgent struct {
	browser        *rod.Browser
	chromeLauncher *launcher.Launcher // nil when attached via ChromeWSUrl
	cdpWS          *cdp.WebSocket
	tempDir        string
	ownsTempDir    bool
}

// Crawler is a standard crawler instance
type Crawler struct {
	*common.Shared

	// TODO: Remove the Chrome PID kill code in favor of using Leakless(true).
	// This change will be made if there are no complaints about zombie Chrome processes.
	// References:
	// https://github.com/projectdiscovery/katana/issues/632
	// https://github.com/projectdiscovery/httpx/issues/1425
	// previousPIDs map[int32]struct{} // track already running PIDs

	agents []*browserAgent
	// browser is the first agent, kept for callers/tests that expect a primary handle.
	browser *rod.Browser
}

// New returns a new standard crawler instance
func New(options *types.CrawlerOptions) (*Crawler, error) {
	agentsCount := hybridBrowserAgents(options.Options)
	if agentsCount > 1 {
		gologger.Info().Msgf("hybrid: using %d browser agents (from -c, max %d)", agentsCount, maxHybridBrowserAgents)
	}

	agents := make([]*browserAgent, 0, agentsCount)
	cleanup := func() {
		for _, a := range agents {
			_ = a.close()
		}
	}

	for i := 0; i < agentsCount; i++ {
		agent, err := launchBrowserAgent(options, i)
		if err != nil {
			cleanup()
			return nil, err
		}
		agents = append(agents, agent)
	}

	shared, err := common.NewShared(options)
	if err != nil {
		cleanup()
		return nil, errkit.Wrap(err, "hybrid")
	}

	crawler := &Crawler{
		Shared:  shared,
		agents:  agents,
		browser: agents[0].browser,
	}
	return crawler, nil
}

// launchBrowserAgent launches one isolated Chrome instance (or attaches to a
// shared one via ChromeWSUrl) and returns the resulting agent handle.
func launchBrowserAgent(options *types.CrawlerOptions, index int) (*browserAgent, error) {
	var dataStore string
	var ownsTempDir bool
	var err error

	if options.Options.ChromeDataDir != "" {
		dataStore = options.Options.ChromeDataDir
	} else {
		dataStore, err = os.MkdirTemp("", fmt.Sprintf("katana-%d-*", index))
		if err != nil {
			return nil, errkit.Wrap(err, "hybrid: could not create temporary directory")
		}
		ownsTempDir = true
	}

	// previousPIDs := processutil.FindProcesses(processutil.IsChromeProcess)

	var launcherURL string
	var chromeLauncher *launcher.Launcher

	if options.Options.ChromeWSUrl != "" {
		launcherURL = options.Options.ChromeWSUrl
	} else {
		// create new chrome launcher instance
		chromeLauncher, err = buildChromeLauncher(options, dataStore)
		if err != nil {
			if ownsTempDir {
				_ = os.RemoveAll(dataStore)
			}
			return nil, err
		}

		// launch chrome headless process
		launcherURL, err = chromeLauncher.Launch()
		if err != nil {
			if ownsTempDir {
				_ = os.RemoveAll(dataStore)
			}
			return nil, err
		}
	}

	// Construct the CDP client here rather than using rod.New().ControlURL(...)
	// so the websocket handle survives into close(). rod hides it in an
	// unexported field, and without it the connection can never be closed --
	// see close() for why that leaks. StartWithURL, which Connect calls, is
	// exactly this.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cdpWS := &cdp.WebSocket{}
	wsErr := cdpWS.Connect(dialCtx, launcherURL, nil)
	dialCancel()
	if wsErr != nil {
		if chromeLauncher != nil {
			chromeLauncher.Kill()
		}
		if ownsTempDir {
			_ = os.RemoveAll(dataStore)
		}
		return nil, errkit.Wrap(wsErr, fmt.Sprintf("hybrid: failed to connect to chrome instance at %s", launcherURL))
	}
	browser := rod.New().Client(cdp.New().Start(cdpWS))
	if browserErr := browser.Connect(); browserErr != nil {
		_ = cdpWS.Close()
		if chromeLauncher != nil {
			chromeLauncher.Kill()
		}
		if ownsTempDir {
			_ = os.RemoveAll(dataStore)
		}
		return nil, errkit.Wrap(browserErr, fmt.Sprintf("hybrid: failed to connect to chrome instance at %s", launcherURL))
	}

	owned := false
	defer func() {
		if owned {
			return
		}
		closeBrowser(browser, chromeLauncher != nil)
		_ = cdpWS.Close()
		if chromeLauncher != nil {
			chromeLauncher.Kill()
		}
		if ownsTempDir {
			_ = os.RemoveAll(dataStore)
		}
	}()

	// create a new browser instance (default to incognito mode)
	if !options.Options.HeadlessNoIncognito {
		// Create the browser context directly rather than via browser.Incognito():
		// rod's helper takes no proxy argument, and Options.Proxy otherwise never
		// reaches a browser attached through ChromeWSUrl, because the chrome
		// launcher -- its only proxy path -- does not run in that case.
		res, err := proto.TargetCreateBrowserContext{ProxyServer: options.Options.Proxy}.Call(browser)
		if err != nil {
			return nil, errkit.Wrap(err, "hybrid: failed to create incognito browser")
		}
		incognito := *browser
		incognito.BrowserContextID = res.BrowserContextID
		browser = &incognito
	}

	agent := &browserAgent{
		browser:        browser,
		chromeLauncher: chromeLauncher,
		cdpWS:          cdpWS,
		// previousPIDs: previousPIDs,
		tempDir:     dataStore,
		ownsTempDir: ownsTempDir,
	}
	owned = true

	return agent, nil
}

// shouldCloseBrowser reports whether it is safe to call rod.Browser.Close()
// on a browser handle. Without an incognito context (hasContext == false),
// rod issues the CDP Browser.close command instead of
// TargetDisposeBrowserContext -- correct for a browser katana launched
// itself (owned == true), but for one attached via -chrome-ws-url
// (owned == false) that would terminate a shared browser other crawls, or
// the browser's actual owner, still need. When there IS an incognito
// context, Close() only disposes that context and is always safe.
func shouldCloseBrowser(hasContext, owned bool) bool {
	return hasContext || owned
}

// closeBrowser closes a browser handle per shouldCloseBrowser.
func closeBrowser(browser *rod.Browser, owned bool) {
	if browser == nil {
		return
	}
	if !shouldCloseBrowser(browser.BrowserContextID != "", owned) {
		return
	}
	_ = browser.Close()
}

func (a *browserAgent) close() error {
	if a == nil {
		return nil
	}
	closeBrowser(a.browser, a.chromeLauncher != nil)
	if a.cdpWS != nil {
		// Close AFTER browser.Close, which dispatches
		// Target.disposeBrowserContext over this same socket.
		//
		// rod's initEvents goroutine blocks on `for e := range client.Event()`,
		// and that channel closes only when cdp's read loop errors -- which
		// happens only when the CONNECTION closes, not on context cancellation
		// (the websocket Read is a blocking socket read). Disposing the browser
		// context does not close the connection, so against a browser attached
		// via ChromeWSUrl -- where there is no launcher to kill -- every crawl
		// left the socket and its goroutines behind for the browser's lifetime.
		_ = a.cdpWS.Close()
	}
	if a.chromeLauncher != nil {
		a.chromeLauncher.Kill()
	}
	if a.ownsTempDir && a.tempDir != "" {
		if err := os.RemoveAll(a.tempDir); err != nil {
			return err
		}
	}
	// processutil.CloseProcesses(processutil.IsChromeProcess, a.previousPIDs)
	return nil
}

// Close closes the crawler process
func (c *Crawler) Close() error {
	var firstErr error
	for _, a := range c.agents {
		if err := a.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Crawl crawls a URL with the specified options
func (c *Crawler) Crawl(rootURL string) error {
	crawlSession, err := c.NewCrawlSessionWithURL(rootURL)
	if err != nil {
		return errkit.Wrap(err, "hybrid")
	}
	crawlSession.Browser = c.browser

	defer crawlSession.CancelFunc()

	gologger.Info().Msgf("Started headless crawling for => %v", rootURL)
	if err := c.Do(crawlSession, c.navigateRequest); err != nil {
		return errkit.Wrap(err, "hybrid")
	}
	return nil
}

// Do executes the crawling loop with one page at a time per browser agent.
// Multiple agents (from -c) each own an isolated Chrome process so CDP work
// does not contend on a single browser target.
//
// The queue's PopWithContext channel closes after it has observed no new
// items for its idle timeout, which happens routinely here: browser
// navigation for in-flight requests can easily take longer than that
// timeout while the queue is otherwise empty. Draining is therefore done in
// rounds: whenever a round ends with in-flight goroutines still running, we
// wait for them (they may enqueue more work) and open a fresh
// PopWithContext to drain whatever they added, instead of exiting and
// silently dropping items enqueued after the channel closed.
func (c *Crawler) Do(crawlSession *common.CrawlSession, doRequest common.DoRequestFunc) error {
	agents := c.agents
	if len(agents) == 0 {
		return errkit.New("hybrid: no browser agents available")
	}

	browserCh := make(chan *rod.Browser, len(agents))
	for _, a := range agents {
		browserCh <- a.browser
	}

	wg := sizedwaitgroup.New(len(agents))
	var inFlight int64

	for {
		if crawlSession.Ctx.Err() != nil {
			break
		}

		for item := range crawlSession.Queue.PopWithContext(crawlSession.Ctx) {
			if crawlSession.Ctx.Err() != nil {
				break
			}

			req, ok := item.(*navigation.Request)
			if !ok {
				continue
			}

			if !utils.IsURL(req.URL) {
				if c.Options.Options.OnSkipURL != nil {
					c.Options.Options.OnSkipURL(req.URL)
				}
				gologger.Debug().Msgf("`%v` not a url. skipping", req.URL)
				continue
			}

			if !c.Options.ValidatePath(req.URL) {
				gologger.Debug().Msgf("`%v` filtered path. skipping", req.URL)
				continue
			}

			inScope, scopeErr := c.Options.ValidateScope(req.URL, crawlSession.Hostname)
			if scopeErr != nil {
				gologger.Debug().Msgf("Error validating scope for `%v`: %v. skipping", req.URL, scopeErr)
				continue
			}
			if !req.SkipValidation && !inScope {
				gologger.Debug().Msgf("`%v` not in scope. skipping", req.URL)
				continue
			}

			atomic.AddInt64(&inFlight, 1)
			wg.Add()
			go func(req *navigation.Request, inScope bool) {
				defer wg.Done()
				defer atomic.AddInt64(&inFlight, -1)

				select {
				case <-crawlSession.Ctx.Done():
					return
				case browser := <-browserCh:
					defer func() { browserCh <- browser }()

					// Race Take() against the session context so the loop doesn't
					// block on a limiter tick when the crawl has been cancelled.
					//
					// Note: when the session is cancelled mid-Take, this inner
					// goroutine outlives the loop iteration and stays blocked on
					// the limiter until the next tick or until RateLimit.Stop() is
					// called by CrawlerOptions.Close(). The leak is bounded by
					// Close() and acceptable.
					takeDone := make(chan struct{})
					go func() {
						if c.Options.HostRateLimit != nil {
							_ = c.Options.HostRateLimit.Take(crawlSession.Hostname)
						} else if c.Options.RateLimit != nil {
							c.Options.RateLimit.Take()
						}
						close(takeDone)
					}()
					select {
					case <-crawlSession.Ctx.Done():
						return
					case <-takeDone:
					}
					c.ApplyBackoff(crawlSession.Hostname)

					if crawlSession.Ctx.Err() != nil {
						return
					}

					if c.Options.Options.Delay > 0 {
						select {
						case <-crawlSession.Ctx.Done():
							return
						case <-time.After(time.Duration(c.Options.Options.Delay) * time.Second):
						}
					}

					if c.Options.Options.MaxDomainPages > 0 {
						counter := c.DomainCounter(crawlSession.Hostname)
						if counter.Add(1) > int64(c.Options.Options.MaxDomainPages) {
							return
						}
					}

					session := *crawlSession
					session.Browser = browser

					resp, err := doRequest(&session, req)

					if resp != nil && common.IsThrottled(resp.StatusCode) {
						c.RecordThrottle(crawlSession.Hostname, resp.StatusCode)
					} else if resp != nil {
						c.RecordSuccess(crawlSession.Hostname)
					}

					if inScope {
						c.Output(req, resp, err)
					}

					if err != nil {
						gologger.Warning().Msgf("Could not request seed URL %s: %s\n", req.URL, err)
						outputError := &output.Error{
							Timestamp: time.Now(),
							Endpoint:  req.RequestURL(),
							Source:    req.Source,
							Error:     err.Error(),
						}
						_ = c.Options.OutputWriter.WriteErr(outputError)
						return
					}
					if resp == nil || resp.Resp == nil || resp.Reader == nil {
						return
					}
					if c.Options.Options.DisableRedirects && resp.IsRedirect() {
						return
					}

					navigationRequests := c.Options.Parser.ParseResponse(resp)
					c.Enqueue(crawlSession.Queue, navigationRequests...)
				}
			}(req, inScope)
		}

		if crawlSession.Ctx.Err() != nil {
			break
		}
		if atomic.LoadInt64(&inFlight) == 0 {
			// Queue is idle and nothing is left running to enqueue more
			// work: the crawl is genuinely done.
			break
		}
		// In-flight goroutines may still enqueue work; wait for the current
		// batch to settle, then drain again.
		wg.Wait()
	}
	wg.Wait()

	if err := crawlSession.Ctx.Err(); err != nil {
		return err
	}
	return nil
}

// buildChromeLauncher builds a new chrome launcher instance
func buildChromeLauncher(options *types.CrawlerOptions, dataStore string) (*launcher.Launcher, error) {
	chromeLauncher := launcher.New().
		Leakless(true).
		Set("disable-gpu", "true").
		Set("ignore-certificate-errors", "true").
		Set("ignore-certificate-errors", "1").
		Set("disable-crash-reporter", "true").
		Set("disable-notifications", "true").
		Set("hide-scrollbars", "true").
		Set("window-size", fmt.Sprintf("%d,%d", 1080, 1920)).
		Set("mute-audio", "true").
		Delete("use-mock-keychain").
		UserDataDir(dataStore)

	if options.Options.UseInstalledChrome {
		if options.Options.SystemChromePath != "" {
			chromeLauncher.Bin(options.Options.SystemChromePath)
		} else {
			if chromePath, hasChrome := launcher.LookPath(); hasChrome {
				chromeLauncher.Bin(chromePath)
			} else {
				return nil, errkit.New("hybrid: the chrome browser is not installed")
			}
		}
	}
	if options.Options.SystemChromePath != "" {
		chromeLauncher.Bin(options.Options.SystemChromePath)
	}

	if options.Options.ShowBrowser {
		chromeLauncher = chromeLauncher.Headless(false)
	} else {
		chromeLauncher = chromeLauncher.Headless(true)
	}

	if options.Options.HeadlessNoSandbox {
		chromeLauncher.Set("no-sandbox", "true")
	}

	if options.Options.Proxy != "" && options.Options.Headless {
		proxyURL, err := urlutil.Parse(options.Options.Proxy)
		if err != nil {
			return nil, err
		}
		chromeLauncher.Set("proxy-server", proxyURL.String())
	}

	for k, v := range options.Options.ParseHeadlessOptionalArguments() {
		chromeLauncher.Set(flags.Flag(k), v)
	}

	return chromeLauncher, nil
}
