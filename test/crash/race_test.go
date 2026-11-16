//go:build race

package crash

// raceBuild lowers the default cycle count: the race detector makes every
// write several times slower. The gate run (make crash) sets -cycles.
const raceBuild = true
