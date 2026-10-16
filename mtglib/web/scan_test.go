package web

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randomB64(t *testing.T) string {
	t.Helper()

	raw := make([]byte, 32)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestScannerFindsSecrets(t *testing.T) {
	t.Parallel()

	signer := newTestSigner(t, "scan")
	tok, _, err := signer.issue(tokenSession)
	require.NoError(t, err)

	capability := randomB64(t)
	scanner := newSecretScanner(signer, []string{capability, "short", "!!notbase64!!"})

	build := func(target string, headers map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}

		return r
	}

	tests := map[string]struct {
		req  *http.Request
		want bool
	}{
		"capability query":    {build("/?bridge="+capability, nil), true},
		"capability mid":      {build("/x?a="+capability+"&b=1", nil), true},
		"bearer":              {build("/", map[string]string{"Authorization": "Bearer " + tok}), true},
		"percent encoded":     {build("/", map[string]string{"X-Foo": "zz%" + "41" + url.PathEscape(tok)}), true},
		"percent in token":    {build("/", map[string]string{"X-Foo": percentEncodeAll(tok)}), true},
		"cookie":              {build("/", map[string]string{"Cookie": "s=" + tok}), true},
		"random query":        {build("/?bridge="+randomB64(t), nil), false},
		"random bearer":       {build("/", map[string]string{"Authorization": "Bearer " + randomB64(t)}), false},
		"short prefix":        {build("/", map[string]string{"X-Foo": tok[:42]}), false},
		"adjacent chars":      {build("/", map[string]string{"X-Foo": "AAAA" + tok + "BBBB"}), false},
		"no headers":          {build("/", nil), false},
		"capability in path":  {build("/"+capability, nil), true},
		"capability w/ extra": {build("/?q=-"+capability, nil), true},
	}

	// "adjacent chars" is a run longer than 43; the window scan still finds T.
	tests["adjacent chars"] = struct {
		req  *http.Request
		want bool
	}{tests["adjacent chars"].req, true}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, scanner.requestCarriesSecret(tt.req))
		})
	}

	t.Run("header name after canonicalization", func(t *testing.T) {
		t.Parallel()

		// net/http canonicalizes header names (textproto), which destroys
		// the case of base64, so a token used as a header name is not
		// recoverable. The same token as a value is detected.
		asName := "GET / HTTP/1.1\r\nHost: example.com\r\n" + tok + ": 1\r\n\r\n"
		r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(asName)))
		require.NoError(t, err)
		assert.False(t, scanner.requestCarriesSecret(r))

		asValue := "GET / HTTP/1.1\r\nHost: example.com\r\nX-Foo: " + tok + "\r\n\r\n"
		r, err = http.ReadRequest(bufio.NewReader(strings.NewReader(asValue)))
		require.NoError(t, err)
		assert.True(t, scanner.requestCarriesSecret(r))
	})

	t.Run("header name verbatim", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header[tok] = []string{"1"}

		assert.True(t, scanner.requestCarriesSecret(r))
	})

	t.Run("malformed escape before encoded token", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Foo", "%zz"+percentEncodeAll(tok))

		assert.True(t, scanner.requestCarriesSecret(r))
	})

	t.Run("malformed escape alone", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Foo", "%zz")

		assert.False(t, scanner.requestCarriesSecret(r))
	})

	t.Run("second header value", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Add("X-Foo", "a")
		r.Header.Add("X-Foo", tok)

		assert.True(t, scanner.requestCarriesSecret(r))
	})

	t.Run("host", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Host = capability + ".example.com"

		assert.True(t, scanner.requestCarriesSecret(r))
	})

	t.Run("1000 random headers", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		for range 1000 {
			r.Header.Add("X-"+randomB64(t)[:8], randomB64(t))
		}

		assert.False(t, scanner.requestCarriesSecret(r))
	})
}

func percentEncodeAll(s string) string {
	const hexdigits = "0123456789ABCDEF"

	out := make([]byte, 0, len(s)*3)
	for i := range len(s) {
		out = append(out, '%', hexdigits[s[i]>>4], hexdigits[s[i]&15])
	}

	return string(out)
}
