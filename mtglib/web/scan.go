package web

import (
	"crypto/subtle"
	"encoding/base64"
	"net/http"
)

// secretScanner detects tokens and capabilities in an HTTP request.
//
// Header names are scanned on a best-effort basis only: net/http
// canonicalizes names (textproto), which destroys base64 case, so a token
// placed in a name is usually unrecoverable. Only a name which survives
// canonicalization unchanged can match.
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
	if s.scanString(input) {
		return true
	}

	decoded := lenientUnescape(input)

	return decoded != input && s.scanString(decoded)
}

// lenientUnescape decodes %XX sequences with valid hex digits and copies
// everything else verbatim, so a malformed escape does not disable decoding
// of the rest of the input.
func lenientUnescape(input string) string {
	out := make([]byte, 0, len(input))

	for i := 0; i < len(input); i++ {
		if input[i] == '%' && i+2 < len(input) && unhex(input[i+1]) >= 0 && unhex(input[i+2]) >= 0 {
			out = append(out, byte(unhex(input[i+1])<<4|unhex(input[i+2])))
			i += 2

			continue
		}

		out = append(out, input[i])
	}

	return string(out)
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}

	return -1
}

func (s *secretScanner) scanString(input string) bool {
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
