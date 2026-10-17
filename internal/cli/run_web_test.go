package cli

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/internal/config"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/mtglib/web"
	"github.com/stretchr/testify/require"
)

const webTestIndex = "<html>hello from public dir</html>"

type webTestAllowAll struct{}

func (webTestAllowAll) Contains(net.IP) bool { return true }
func (webTestAllowAll) Run(time.Duration)    {}
func (webTestAllowAll) Shutdown()            {}

func webTestPublicDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte(webTestIndex), 0o600))

	return dir
}

func webTestConfig(t *testing.T, extra string) *config.Config {
	t.Helper()

	conf, err := config.Parse([]byte(`
secret = "7oe1GqLy6TBc38CV3jx7q09nb29nbGUuY29t"
bind-to = "0.0.0.0:3128"
` + extra))
	require.NoError(t, err)
	require.NoError(t, conf.Validate())

	return conf
}

func webTestEnabledConfig(t *testing.T) *config.Config {
	t.Helper()

	return webTestConfig(t, `
[web]
enabled = true
bind-to = "127.0.0.1:8443"
hostname = "web.example.com"
public-dir = "`+webTestPublicDir(t)+`"
`)
}

func webTestProxy(t *testing.T, conf *config.Config) *mtglib.Proxy {
	t.Helper()

	ntw, err := makeNetwork(conf, "test")
	require.NoError(t, err)

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Logger:          logger.NewNoopLogger(),
		Network:         ntw,
		AntiReplayCache: antireplay.NewNoop(),
		IPBlocklist:     ipblocklist.NewNoop(),
		IPAllowlist:     webTestAllowAll{},
		EventStream:     events.NewNoopStream(),
		Secret:          conf.Secret,
	})
	require.NoError(t, err)
	t.Cleanup(proxy.Shutdown)

	return proxy
}

func TestMakeWebHandlerDisabled(t *testing.T) {
	conf := webTestConfig(t, "")

	handler, err := makeWebHandler(conf, webTestProxy(t, conf), logger.NewNoopLogger())
	require.NoError(t, err)
	require.Nil(t, handler)
}

func TestMakeWebHandlerFromConfig(t *testing.T) {
	conf := webTestEnabledConfig(t)

	handler, err := makeWebHandler(conf, webTestProxy(t, conf), logger.NewNoopLogger())
	require.NoError(t, err)
	require.NotNil(t, handler)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		handler.Shutdown(ctx) //nolint: errcheck
	})

	require.Equal(t, web.Capability(conf.Secret, "web.example.com"), handler.Capability(0))
}

func TestServeWebRoundTrip(t *testing.T) {
	conf := webTestEnabledConfig(t)
	log := logger.NewNoopLogger()

	handler, err := makeWebHandler(conf, webTestProxy(t, conf), log)
	require.NoError(t, err)
	require.NotNil(t, handler)

	// web.bind-to cannot be port 0, so pass an own ephemeral listener.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	shutdown := serveWebListener(t.Context(), conf, handler, listener, log)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+listener.Addr().String()+"/", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close() //nolint: errcheck
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, webTestIndex, string(body))

	done := make(chan struct{})

	go func() {
		shutdown()
		shutdown() // idempotent
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return within 2s")
	}
}

func TestServeWebDisabled(t *testing.T) {
	conf := webTestConfig(t, "")

	shutdown, err := serveWeb(t.Context(), conf, nil, logger.NewNoopLogger())
	require.NoError(t, err)
	require.Nil(t, shutdown)
}
