//go:build race

package lsm

// raceEnabled lowers the default size of the long model test under the
// race detector; the gate run sets the size explicitly.
const raceEnabled = true
