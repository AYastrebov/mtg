//go:build !race

package web

// raceEnabled reports whether the race detector is on: timing tests skip
// themselves under its overhead.
const raceEnabled = false
