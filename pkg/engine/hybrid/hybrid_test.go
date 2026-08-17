package hybrid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldCloseBrowser(t *testing.T) {
	t.Parallel()

	t.Run("owned browser without incognito context is closed", func(t *testing.T) {
		t.Parallel()
		// katana launched the chrome process itself (e.g. -headless-no-incognito
		// without -chrome-ws-url): safe to send Browser.close.
		require.True(t, shouldCloseBrowser(false, true))
	})

	t.Run("owned browser with incognito context is closed", func(t *testing.T) {
		t.Parallel()
		require.True(t, shouldCloseBrowser(true, true))
	})

	t.Run("borrowed browser with incognito context is closed", func(t *testing.T) {
		t.Parallel()
		// -chrome-ws-url without -headless-no-incognito: Close() only disposes
		// the incognito context, never the shared browser.
		require.True(t, shouldCloseBrowser(true, false))
	})

	t.Run("borrowed browser without incognito context is not closed", func(t *testing.T) {
		t.Parallel()
		// -chrome-ws-url combined with -headless-no-incognito: katana neither
		// launched this browser nor holds an isolated context to dispose of,
		// so Close() must not be called (it would send Browser.close and
		// terminate a browser katana does not own). Regression test for
		// https://github.com/projectdiscovery/katana/issues/1757
		require.False(t, shouldCloseBrowser(false, false))
	})
}
