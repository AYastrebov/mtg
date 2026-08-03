package fake_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/mtglib/internal/tls/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The snapshot fixtures are real ClientHellos recorded against this secret, so
// they are the cheapest source of a handshake that genuinely verifies. Using
// them means these tests exercise the same bytes a Telegram client sends,
// rather than something forged to match the implementation.
const multiSecretFixture = "ee367a189aee18fa31c190054efd4a8e9573746f726167652e676f6f676c65617069732e636f6d"

func multiSecret(t *testing.T) mtglib.Secret {
	t.Helper()

	parsed, err := mtglib.ParseSecret(multiSecretFixture)
	require.NoError(t, err)

	return parsed
}

// multiSecretKeyAt builds a 16-byte key that cannot collide with the fixture's,
// so a match can only mean the right candidate was picked.
func multiSecretKeyAt(n byte) []byte {
	key := make([]byte, mtglib.SecretKeyLength)
	for i := range key {
		key[i] = n
	}

	return key
}

// multiSecretConn returns a fresh connection carrying a known-good ClientHello.
// Fresh per call: the parser consumes the buffer.
func multiSecretConn(t *testing.T) *parseClientHelloConnMock {
	t.Helper()

	files, err := os.ReadDir("testdata")
	require.NoError(t, err)

	for _, v := range files {
		if !strings.HasPrefix(v.Name(), "client-hello-ok") {
			continue
		}

		data, err := os.ReadFile(filepath.Join("testdata", v.Name()))
		require.NoError(t, err)

		snapshot := &clientHelloSnapshot{}
		require.NoError(t, json.Unmarshal(data, snapshot))

		return &parseClientHelloConnMock{readBuf: bytes.NewBuffer(snapshot.GetFull())}
	}

	t.Fatal("no client-hello-ok fixture in testdata")

	return nil
}

func TestReadClientHelloMatchesASoleSecret(t *testing.T) {
	secret := multiSecret(t)

	hello, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{secret.Key[:]},
		secret.Host,
		TolerateTime,
	)

	require.NoError(t, err)
	assert.NotNil(t, hello)
	assert.Equal(t, 0, idx)
}

func TestReadClientHelloMatchesASecretInTheMiddle(t *testing.T) {
	secret := multiSecret(t)

	hello, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{multiSecretKeyAt(1), secret.Key[:], multiSecretKeyAt(3)},
		secret.Host,
		TolerateTime,
	)

	require.NoError(t, err)
	assert.NotNil(t, hello)
	assert.Equal(t, 1, idx, "the reported index must identify which client connected")
}

func TestReadClientHelloMatchesTheLastSecret(t *testing.T) {
	secret := multiSecret(t)

	_, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{multiSecretKeyAt(1), multiSecretKeyAt(2), secret.Key[:]},
		secret.Host,
		TolerateTime,
	)

	require.NoError(t, err)
	assert.Equal(t, 2, idx)
}

func TestReadClientHelloRejectsWhenNoSecretMatches(t *testing.T) {
	secret := multiSecret(t)

	_, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{multiSecretKeyAt(1), multiSecretKeyAt(2)},
		secret.Host,
		TolerateTime,
	)

	// Falling through to ErrBadDigest is what makes the proxy domain-front an
	// unrecognised client instead of answering it.
	require.ErrorIs(t, err, fake.ErrBadDigest)
	assert.Equal(t, -1, idx)
}

func TestReadClientHelloRejectsAnEmptySecretList(t *testing.T) {
	secret := multiSecret(t)

	_, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		nil,
		secret.Host,
		TolerateTime,
	)

	require.ErrorIs(t, err, fake.ErrBadDigest)
	assert.Equal(t, -1, idx)
}

func TestReadClientHelloStillRejectsAStaleTimestamp(t *testing.T) {
	secret := multiSecret(t)

	// The fixtures were recorded years ago; any real tolerance rejects them.
	// This is the check that keeps replayed handshakes from being accepted
	// just because the key still matches.
	_, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{secret.Key[:]},
		secret.Host,
		time.Second,
	)

	require.Error(t, err)
	assert.ErrorContains(t, err, "too old")
	assert.Equal(t, -1, idx)
}

func TestReadClientHelloStillChecksSNIBeforeAnyKey(t *testing.T) {
	secret := multiSecret(t)

	_, idx, err := fake.ReadClientHello(
		multiSecretConn(t),
		[][]byte{secret.Key[:]},
		"not-the-fronting-host.example",
		TolerateTime,
	)

	// One host for the whole list is the design: the SNI check happens once,
	// before any candidate is tried.
	require.Error(t, err)
	assert.ErrorContains(t, err, "cannot find")
	assert.Equal(t, -1, idx)
}
