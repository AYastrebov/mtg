package web

import (
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"net/url"
)

type secretScanner struct {
	signer       *tokenSigner
	capabilities [][]byte
}

// newSecretScanner builds a scanner which detects tokens issued by signer
// and any of capabilities (base64url strings) in a request.
func newSecretScanner(signer *tokenSigner, capabilities []string) *secretScanner {
	decoded := make([][]byte, 0, len(capabilities))

	for _, c := range capabilities {
		raw, err := base64.RawURLEncoding.DecodeString(c)
		if err != nil || len(raw) != tokenRawSize {
			continue
		}

		decoded = append(decoded, raw)
	}

	return &secretScanner{signer: signer, capabilities: decoded}
}

func (s *secretScanner) requestCarriesSecret(r *http.Request) bool {
	if s.scan(r.URL.RequestURI()) || s.scan(r.Host) {
		return true
	}

	for name, values := range r.Header {
		if s.scan(name) {
			return true
		}

		for _, v := range values {
			if s.scan(v) {
				return true
			}
		}
	}

	return false
}

func isTokenChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '_'
}

func (s *secretScanner) scan(input string) bool {
	if decoded, err := url.PathUnescape(input); err == nil {
		input = decoded
	}

	start := 0

	for i := 0; i <= len(input); i++ {
		if i < len(input) && isTokenChar(input[i]) {
			continue
		}

		if i-start >= tokenEncodedSize && s.scanRun(input[start:i]) {
			return true
		}

		start = i + 1
	}

	return false
}

func (s *secretScanner) scanRun(run string) bool {
	for i := 0; i+tokenEncodedSize <= len(run); i++ {
		raw, err := base64.RawURLEncoding.DecodeString(run[i : i+tokenEncodedSize])
		if err != nil {
			continue
		}

		if _, _, ok := s.signer.classify(raw); ok {
			return true
		}

		for _, c := range s.capabilities {
			if subtle.ConstantTimeCompare(raw, c) == 1 {
				return true
			}
		}
	}

	return false
}
