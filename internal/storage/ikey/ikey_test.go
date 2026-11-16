package ikey

import (
	"slices"
	"testing"
)

func TestTrailerRoundTrip(t *testing.T) {
	for _, seq := range []uint64{0, 1, 255, 256, 1 << 40, MaxSeq} {
		for _, kind := range []Kind{KindDelete, KindSet} {
			tr := MakeTrailer(seq, kind)
			if tr.Seq() != seq || tr.Kind() != kind {
				t.Errorf("MakeTrailer(%d, %s) = seq %d kind %s", seq, kind, tr.Seq(), tr.Kind())
			}
		}
	}
}

func TestCompareOrder(t *testing.T) {
	// want lists keys in the exact order Compare must produce.
	want := []Key{
		{UserKey: []byte(""), Trailer: MakeTrailer(9, KindSet)},
		{UserKey: []byte("a"), Trailer: MakeTrailer(MaxSeq, KindSet)},
		{UserKey: []byte("a"), Trailer: MakeTrailer(7, KindSet)},
		{UserKey: []byte("a"), Trailer: MakeTrailer(7, KindDelete)},
		{UserKey: []byte("a"), Trailer: MakeTrailer(1, KindSet)},
		{UserKey: []byte("a\x00"), Trailer: MakeTrailer(100, KindSet)},
		{UserKey: []byte("b"), Trailer: MakeTrailer(3, KindDelete)},
	}
	got := slices.Clone(want)
	slices.Reverse(got)
	slices.SortFunc(got, Compare)
	for i := range want {
		if Compare(got[i], want[i]) != 0 {
			t.Fatalf("position %d: got %s, want %s", i, got[i], want[i])
		}
	}
	for i := 0; i+1 < len(want); i++ {
		if Compare(want[i], want[i+1]) != -1 || Compare(want[i+1], want[i]) != 1 {
			t.Errorf("Compare(%s, %s) not strictly ordered", want[i], want[i+1])
		}
	}
}

func TestSeekKeyBracketsVisibleVersions(t *testing.T) {
	uk := []byte("k")
	seek := SeekKey(uk, 10)
	for _, tc := range []struct {
		seq   uint64
		kind  Kind
		after bool // sorts at or after the seek key, i.e. visible at seq 10
	}{
		{11, KindDelete, false},
		{11, KindSet, false},
		{10, KindSet, true},
		{10, KindDelete, true},
		{1, KindSet, true},
	} {
		k := Key{UserKey: uk, Trailer: MakeTrailer(tc.seq, tc.kind)}
		if got := Compare(k, seek) >= 0; got != tc.after {
			t.Errorf("%s vs SeekKey(k, 10): at-or-after = %v, want %v", k, got, tc.after)
		}
	}
}

func TestKindValid(t *testing.T) {
	if !KindDelete.Valid() || !KindSet.Valid() || Kind(2).Valid() {
		t.Fatal("Kind.Valid disagrees with the defined kinds")
	}
}
