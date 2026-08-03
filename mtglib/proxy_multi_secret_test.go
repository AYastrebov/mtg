package mtglib_test

import (
	"testing"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// multiSecretOpts builds the minimum viable ProxyOpts. Every mandatory
// dependency is a noop so these tests exercise validation, not networking.
func multiSecretOpts(t *testing.T) mtglib.ProxyOpts {
	t.Helper()

	dialer, err := network.NewDefaultDialer(0, 0)
	require.NoError(t, err)

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	require.NoError(t, err)

	return mtglib.ProxyOpts{
		Network:         ntw,
		AntiReplayCache: antireplay.NewNoop(),
		IPBlocklist:     ipblocklist.NewNoop(),
		IPAllowlist:     ipblocklist.NewNoop(),
		EventStream:     events.NewNoopStream(),
		Logger:          logger.NewNoopLogger(),
		UseTestDCs:      true,
	}
}

func TestNewProxyAcceptsASecretList(t *testing.T) {
	opts := multiSecretOpts(t)
	opts.Secrets = []mtglib.Secret{
		mtglib.GenerateSecret("httpbin.org"),
		mtglib.GenerateSecret("httpbin.org"),
	}

	proxy, err := mtglib.NewProxy(opts)

	require.NoError(t, err)
	require.NotNil(t, proxy)

	proxy.Shutdown()
}

func TestNewProxyStillAcceptsASingleSecret(t *testing.T) {
	opts := multiSecretOpts(t)
	opts.Secret = mtglib.GenerateSecret("httpbin.org")

	proxy, err := mtglib.NewProxy(opts)

	require.NoError(t, err)
	require.NotNil(t, proxy)

	proxy.Shutdown()
}

func TestNewProxyPrefersSecretsOverSecret(t *testing.T) {
	listed := mtglib.GenerateSecret("httpbin.org")

	opts := multiSecretOpts(t)
	opts.Secret = mtglib.GenerateSecret("other.example")
	opts.Secrets = []mtglib.Secret{listed}

	proxy, err := mtglib.NewProxy(opts)

	require.NoError(t, err)
	require.NotNil(t, proxy)

	defer proxy.Shutdown()

	// The fronting address comes off the secret list, proving Secret was not
	// silently mixed in — if it were, the hosts would disagree and the two
	// could not both be valid.
	assert.Contains(t, proxy.DomainFrontingAddress(), "httpbin.org")
}

func TestNewProxyRejectsNoSecretAtAll(t *testing.T) {
	opts := multiSecretOpts(t)

	_, err := mtglib.NewProxy(opts)

	assert.ErrorIs(t, err, mtglib.ErrSecretInvalid)
}

func TestNewProxyRejectsAListContainingAnInvalidSecret(t *testing.T) {
	opts := multiSecretOpts(t)
	opts.Secrets = []mtglib.Secret{
		mtglib.GenerateSecret("httpbin.org"),
		{}, // zero value: no key, no host
	}

	_, err := mtglib.NewProxy(opts)

	assert.ErrorIs(t, err, mtglib.ErrSecretInvalid)
}

func TestNewProxyRejectsSecretsWithDifferentHosts(t *testing.T) {
	opts := multiSecretOpts(t)
	opts.Secrets = []mtglib.Secret{
		mtglib.GenerateSecret("httpbin.org"),
		mtglib.GenerateSecret("other.example"),
	}

	_, err := mtglib.NewProxy(opts)

	// The SNI check runs once, before any key is tried, so a secret with a
	// different host could never authenticate. Better to say so at construction
	// than to let that client fail forever at runtime.
	assert.ErrorIs(t, err, mtglib.ErrSecretHostMismatch)
}
