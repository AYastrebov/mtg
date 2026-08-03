package mtglib_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The FakeTLS ClientHello layout, mirrored from internal/tls/fake so this test
// can build a handshake without reaching into an internal package:
// record_type(1) + version(2) + size(2) + handshake_type(1) + uint24_length(3)
// + client_version(2) = 11 bytes before the 32-byte client random.
const (
	e2eRandomOffset = 11
	e2eRandomLen    = 32

	// The SNI baked into the recorded fixtures.
	e2eFrontingHost = "storage.googleapis.com"
)

// signClientHello turns a recorded ClientHello into one that authenticates
// against an arbitrary key, which is what a real client does: HMAC the frame
// with the client random zeroed, then place the digest — with the timestamp
// folded into its last four bytes — into the random field.
func signClientHello(t *testing.T, template []byte, key []byte, at time.Time) []byte {
	t.Helper()

	out := make([]byte, len(template))
	copy(out, template)

	// The digest is taken over the frame with the random zeroed.
	for i := range e2eRandomLen {
		out[e2eRandomOffset+i] = 0
	}

	digest := hmac.New(sha256.New, key)
	digest.Write(out)
	computed := digest.Sum(nil)

	// The verifier XORs the random back out of the digest and expects zeroes,
	// except for a little-endian unix timestamp in the final four bytes.
	stamp := make([]byte, 4)
	binary.LittleEndian.PutUint32(stamp, uint32(at.Unix()))

	for i := range e2eRandomLen {
		b := computed[i]
		if i >= e2eRandomLen-4 {
			b ^= stamp[i-(e2eRandomLen-4)]
		}

		out[e2eRandomOffset+i] = b
	}

	return out
}

// clientHelloTemplate returns the bytes of a recorded, structurally valid
// ClientHello. Only its shape is reused; the signature is replaced.
func clientHelloTemplate(t *testing.T) []byte {
	t.Helper()

	dir := filepath.Join("internal", "tls", "fake", "testdata")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range entries {
		if !isOkFixture(e.Name()) {
			continue
		}

		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)

		var snapshot struct {
			Full string `json:"full"`
		}
		require.NoError(t, json.Unmarshal(raw, &snapshot))

		full, err := base64.StdEncoding.DecodeString(snapshot.Full)
		require.NoError(t, err)

		return full
	}

	t.Fatal("no client-hello-ok fixture found")

	return nil
}

func isOkFixture(name string) bool {
	const prefix = "client-hello-ok"

	return len(name) >= len(prefix) && name[:len(prefix)] == prefix
}

// allowAll is an allowlist that admits every address.
//
// ipblocklist.NewNoop is not usable here: its Contains reports false, which
// reads as "not blocked" for a blocklist but as "not allowed" for an
// allowlist, so Serve would reject every connection. A Firehol list seeded
// with the whole address space works too, but it populates asynchronously
// inside Run and races a test that dials immediately.
type allowAll struct{}

func (allowAll) Contains(net.IP) bool { return true }
func (allowAll) Run(time.Duration)    {}
func (allowAll) Shutdown()            {}

// matchRecorder captures EventClientMatched so a test can assert which secret
// authenticated a connection.
type matchRecorder struct {
	mu      sync.Mutex
	matched chan int
}

func newMatchRecorder() *matchRecorder {
	return &matchRecorder{matched: make(chan int, 8)}
}

func (m *matchRecorder) Send(_ context.Context, evt mtglib.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ev, ok := evt.(mtglib.EventClientMatched); ok {
		select {
		case m.matched <- ev.SecretIdx:
		default:
		}
	}
}

// await returns the next reported secret index, or fails the test.
func (m *matchRecorder) await(t *testing.T) int {
	t.Helper()

	select {
	case idx := <-m.matched:
		return idx
	case <-time.After(5 * time.Second):
		t.Fatal("no EventClientMatched was emitted")
	}

	return -1
}

// startMultiSecretProxy runs a proxy over the given secrets and returns its
// address plus the recorder watching it.
func startMultiSecretProxy(t *testing.T, secrets []mtglib.Secret) (string, *matchRecorder) {
	t.Helper()

	dialer, err := network.NewDefaultDialer(0, 0)
	require.NoError(t, err)

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	require.NoError(t, err)

	recorder := newMatchRecorder()

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secrets:         secrets,
		Network:         ntw,
		AntiReplayCache: antireplay.NewNoop(),
		IPBlocklist:     ipblocklist.NewNoop(),
		IPAllowlist:     allowAll{},
		EventStream:     recorder,
		Logger:          logger.NewNoopLogger(),
		UseTestDCs:      true,
	})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go proxy.Serve(listener) //nolint: errcheck

	t.Cleanup(func() {
		listener.Close() //nolint: errcheck
		proxy.Shutdown()
	})

	return listener.Addr().String(), recorder
}

// secretsForHost mints n secrets that all front the same host, as a live
// roster would.
func secretsForHost(n int) []mtglib.Secret {
	out := make([]mtglib.Secret, n)
	for i := range out {
		out[i] = mtglib.GenerateSecret(e2eFrontingHost)
	}

	return out
}

// TestProxyAttributesAConnectionToTheMatchingSecret is the contract a panel
// depends on: with several secrets served on one port, a connection is
// attributed to the one it actually authenticated with.
func TestProxyAttributesAConnectionToTheMatchingSecret(t *testing.T) {
	template := clientHelloTemplate(t)
	secrets := secretsForHost(4)

	for _, want := range []int{0, 1, 3} {
		t.Run("secret-"+string(rune('0'+want)), func(t *testing.T) {
			addr, recorder := startMultiSecretProxy(t, secrets)

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			require.NoError(t, err)

			defer conn.Close() //nolint: errcheck

			hello := signClientHello(t, template, secrets[want].Key[:], time.Now())
			_, err = conn.Write(hello)
			require.NoError(t, err)

			assert.Equal(t, want, recorder.await(t))
		})
	}
}

// TestProxyDoesNotAttributeAnUnknownSecret proves the fallback: a client whose
// key is not on the roster is domain-fronted, not charged to somebody else.
func TestProxyDoesNotAttributeAnUnknownSecret(t *testing.T) {
	template := clientHelloTemplate(t)
	secrets := secretsForHost(3)
	stranger := mtglib.GenerateSecret(e2eFrontingHost)

	addr, recorder := startMultiSecretProxy(t, secrets)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)

	defer conn.Close() //nolint: errcheck

	hello := signClientHello(t, template, stranger.Key[:], time.Now())
	_, err = conn.Write(hello)
	require.NoError(t, err)

	select {
	case idx := <-recorder.matched:
		t.Fatalf("an unknown secret was attributed to index %d", idx)
	case <-time.After(time.Second):
		// No match, as intended.
	}
}
