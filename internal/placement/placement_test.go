package placement

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func randomMembers(r *rand.Rand, n int) []string {
	seen := map[string]bool{}
	var out []string
	for len(out) < n {
		m := fmt.Sprintf("10.%d.%d.%d:7100", r.IntN(256), r.IntN(256), r.IntN(256))
		if !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	return out
}

func TestShardOf(t *testing.T) {
	counts := make([]int, 64)
	for i := range 64_000 {
		s := ShardOf([]byte(fmt.Sprintf("key-%d", i)), 64)
		if s < 0 || s >= 64 {
			t.Fatalf("shard %d out of range", s)
		}
		counts[s]++
	}
	for s, c := range counts {
		if c < 800 || c > 1200 { // mean 1000; keys spread evenly over shards
			t.Errorf("shard %d got %d of 64,000 keys", s, c)
		}
	}
	a, b := []byte("same-key"), []byte("same-key") // separate slices, same bytes
	if ShardOf(a, 64) != ShardOf(b, 64) {
		t.Error("ShardOf depends on more than the key's bytes")
	}
}

func TestPlacementIsDeterministic(t *testing.T) {
	a := []string{"n1:7100", "n2:7100", "n3:7100", "n4:7100"}
	b := []string{"n4:7100", "n2:7100", "n1:7100", "n3:7100", "n2:7100"} // other order, a duplicate
	ma, err := Compute(a, 64, 3)
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := Compute(b, 64, 3)
	for s := range 64 {
		set := ma.ReplicaSet(s)
		if !slices.Equal(set, mb.ReplicaSet(s)) {
			t.Fatalf("shard %d: %v vs %v", s, set, mb.ReplicaSet(s))
		}
		if len(set) != 3 || len(slices.Compact(slices.Sorted(slices.Values(set)))) != 3 {
			t.Fatalf("shard %d replica set %v is not 3 distinct members", s, set)
		}
	}
	if _, err := Compute(a[:2], 64, 3); err == nil {
		t.Error("RF 3 on 2 members accepted")
	}
}

// balanceTolerance is the stated tolerance for the worst node's deviation
// from the mean replica count. A node holds each shard's replica with
// probability p = rf/n, so its count is about Binomial(S, p); three standard
// deviations of that, plus 5% for the unevenness of a 128-point-per-member
// ring, covers at least 95% of random clusters.
func balanceTolerance(shards, n, rf int) float64 {
	p := float64(rf) / float64(n)
	return 3*math.Sqrt((1-p)/(float64(shards)*p)) + 0.05
}

// TestBalance is the Phase 5 gate's balance test. For each cluster size and
// shard count, over 1,000 random member sets, the 95th percentile of the
// worst node's deviation from the mean replica count must be within the
// stated tolerance. The results are logged.
func TestBalance(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	const rf, trials = 3, 1000
	for _, shards := range []int{64, 256} {
		for _, n := range []int{3, 4, 5, 7, 10} {
			worst := make([]float64, 0, trials)
			for range trials {
				members := randomMembers(r, n)
				m, err := Compute(members, shards, rf)
				if err != nil {
					t.Fatal(err)
				}
				mean := float64(rf*shards) / float64(n)
				w := 0.0
				for _, x := range members {
					w = max(w, math.Abs(float64(len(m.ShardsOf(x)))-mean)/mean)
				}
				worst = append(worst, w)
			}
			slices.Sort(worst)
			p95, tol := worst[trials*95/100], balanceTolerance(shards, n, rf)
			t.Logf("S=%3d N=%2d RF=%d: worst node off the mean by %4.1f%% (median), %4.1f%% (p95), %4.1f%% (max); tolerance %4.1f%%",
				shards, n, rf, 100*worst[trials/2], 100*p95, 100*worst[trials-1], 100*tol)
			if p95 > tol {
				t.Errorf("S=%d N=%d: p95 deviation %.1f%% exceeds the stated tolerance %.1f%%", shards, n, 100*p95, 100*tol)
			}
		}
	}
}

// TestAddingANodeMovesOnlyItsShare is the Phase 5 gate's movement test.
// Adding a member to N members must change only replica slots that the new
// member takes over: every replica set gains at most the new member and
// loses at most one old member, and primaries only ever move to the new
// member. On average the fraction of replica assignments that change must
// be within 10% of the optimal 1/(N+1).
func TestAddingANodeMovesOnlyItsShare(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	const shards, rf, trials = 64, 3, 400
	for _, n := range []int{3, 4, 5, 7, 9} {
		sum := 0.0
		for range trials {
			members := randomMembers(r, n+1)
			newcomer := members[n]
			before, _ := Compute(members[:n], shards, rf)
			after, _ := Compute(members, shards, rf)
			moved := 0
			for s := range shards {
				old, cur := before.ReplicaSet(s), after.ReplicaSet(s)
				var gained, lost []string
				for _, x := range cur {
					if !slices.Contains(old, x) {
						gained = append(gained, x)
					}
				}
				for _, x := range old {
					if !slices.Contains(cur, x) {
						lost = append(lost, x)
					}
				}
				if len(gained) > 1 || len(lost) != len(gained) || (len(gained) == 1 && gained[0] != newcomer) {
					t.Fatalf("N=%d shard %d: %v -> %v moves slots between old members", n, s, old, cur)
				}
				if before.Primary(s) != after.Primary(s) && after.Primary(s) != newcomer {
					t.Fatalf("N=%d shard %d: primary moved %s -> %s, not to the new member", n, s, before.Primary(s), after.Primary(s))
				}
				moved += len(gained)
			}
			sum += float64(moved) / float64(rf*shards)
		}
		mean, optimal := sum/trials, 1/float64(n+1)
		t.Logf("N=%d->%d: %.1f%% of replica assignments moved on average; optimal %.1f%%", n, n+1, 100*mean, 100*optimal)
		if math.Abs(mean-optimal)/optimal > 0.10 {
			t.Errorf("N=%d: moved %.1f%% on average, optimal is %.1f%%", n, 100*mean, 100*optimal)
		}
	}
}

func TestShardsOfMatchesReplicaSets(t *testing.T) {
	m, _ := Compute([]string{"a:1", "b:1", "c:1", "d:1", "e:1"}, 32, 2)
	total := 0
	for _, x := range []string{"a:1", "b:1", "c:1", "d:1", "e:1"} {
		for _, s := range m.ShardsOf(x) {
			if !slices.Contains(m.ReplicaSet(s), x) {
				t.Fatalf("ShardsOf(%s) lists shard %d, which it does not hold", x, s)
			}
			total++
		}
	}
	if total != 32*2 {
		t.Errorf("replica slots %d, want 64", total)
	}
}

func TestFingerprint(t *testing.T) {
	a := Fingerprint([]string{"b:1", "a:1"}, 64, 3)
	if a != Fingerprint([]string{"a:1", "b:1", "a:1"}, 64, 3) {
		t.Error("fingerprint depends on member order or duplicates")
	}
	for _, other := range []string{
		Fingerprint([]string{"a:1", "c:1"}, 64, 3),
		Fingerprint([]string{"a:1", "b:1"}, 32, 3),
		Fingerprint([]string{"a:1", "b:1"}, 64, 2),
	} {
		if other == a {
			t.Errorf("different placements share fingerprint %s", a)
		}
	}
}
