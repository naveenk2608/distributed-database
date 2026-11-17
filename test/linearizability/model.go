// Package linearizability records client histories against a dkv cluster
// and checks them with Porcupine against a sequential key-value model.
//
// Every operation is recorded with its invocation and response times. An
// operation that failed or timed out has an unknown outcome: it may have
// taken effect at any point after its invocation, or not at all, so it is
// recorded as returning at the end of the history with an unknown output.
// Keys are independent, so the history is checked one key at a time.
package linearizability

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Kind is an operation type.
type Kind int

const (
	Put Kind = iota + 1
	Get
	Delete
	CAS
)

func (k Kind) String() string { return [...]string{"", "put", "get", "delete", "cas"}[k] }

// Input is what an operation asked for.
type Input struct {
	Kind     Kind
	Key      string
	Value    string // put, cas
	Expected uint64 // cas: the version the key must be at (0: absent)
}

// Output is what an operation answered.
type Output struct {
	// Unknown: the operation failed or timed out; it may or may not have
	// taken effect.
	Unknown bool
	Found   bool   // get, delete
	Value   string // get
	// Version: the new version of a write, the version read, or for a
	// failed CAS the key's current version.
	Version   uint64
	Succeeded bool // cas
}

// state is one key's state in the model.
//
// Versions are log indexes, so every write's version exceeds the version
// of the key before it. After a write whose outcome is unknown the key's
// exact version is unknown (versionKnown false), and version holds a
// strict lower bound instead: the version before that write, which the
// write's own version exceeds.
//
// An absent key compares as version 0 in a compare-and-set, whatever
// deleted it.
type state struct {
	present      bool
	value        string
	version      uint64
	versionKnown bool
}

// matches reports whether a compare-and-set expecting version exp can
// find the key at that version, and whether that is certain.
func (s state) matches(exp uint64) (possible, certain bool) {
	switch {
	case !s.present:
		return exp == 0, true
	case s.versionKnown:
		return s.version == exp, true
	default: // present at some version above the bound
		return exp > s.version, false
	}
}

func step(st, in, out any) []any {
	s, i, o := st.(state), in.(Input), out.(Output)
	if o.Unknown {
		// The operation may have taken effect, or not. A write that did
		// leaves the key at an unknown version above the current one.
		unknown := func(present bool, value string, bound uint64) state {
			return state{present: present, value: value, version: bound}
		}
		switch i.Kind {
		case Get:
			return []any{s}
		case Put:
			return []any{s, unknown(true, i.Value, s.version)}
		case Delete:
			return []any{s, unknown(false, "", s.version)}
		case CAS:
			if ok, _ := s.matches(i.Expected); ok {
				return []any{s, unknown(true, i.Value, max(s.version, i.Expected))}
			}
			return []any{s}
		}
		return nil
	}
	// A completed write's version must exceed the key's version (or its
	// lower bound when unknown).
	written := func(present bool, value string) []any {
		if o.Version <= s.version {
			return nil
		}
		return []any{state{present: present, value: value, version: o.Version, versionKnown: true}}
	}
	switch i.Kind {
	case Put:
		return written(true, i.Value)
	case Delete:
		if o.Found != s.present {
			return nil
		}
		return written(false, "")
	case Get:
		if !o.Found {
			if s.present {
				return nil
			}
			return []any{s}
		}
		if !s.present || o.Value != s.value {
			return nil
		}
		if s.versionKnown && o.Version != s.version || !s.versionKnown && o.Version <= s.version {
			return nil
		}
		s.version, s.versionKnown = o.Version, true
		return []any{s}
	case CAS:
		possible, certain := s.matches(i.Expected)
		if o.Succeeded {
			if !possible || o.Version <= i.Expected {
				return nil
			}
			return written(true, i.Value)
		}
		// Failed: the key was not at the expected version, and the answer
		// is its current version (0 if absent).
		if possible && certain {
			return nil
		}
		switch {
		case !s.present:
			if o.Version != 0 {
				return nil
			}
		case s.versionKnown:
			if o.Version != s.version {
				return nil
			}
		default:
			if o.Version == i.Expected || o.Version <= s.version {
				return nil
			}
			s.version, s.versionKnown = o.Version, true
		}
		return []any{s}
	}
	return nil
}

// Model is the Porcupine model of one key-value store.
var Model = porcupine.NondeterministicModel{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range h {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() []any { return []any{state{versionKnown: true}} },
	Step: step,
	DescribeOperation: func(in, out any) string {
		i, o := in.(Input), out.(Output)
		res := "?"
		if !o.Unknown {
			switch i.Kind {
			case Get:
				res = fmt.Sprintf("found=%v %q@%d", o.Found, o.Value, o.Version)
			case CAS:
				res = fmt.Sprintf("ok=%v @%d", o.Succeeded, o.Version)
			case Delete:
				res = fmt.Sprintf("found=%v @%d", o.Found, o.Version)
			default:
				res = fmt.Sprintf("@%d", o.Version)
			}
		}
		arg := i.Key
		if i.Kind == Put || i.Kind == CAS {
			arg += "=" + i.Value
		}
		if i.Kind == CAS {
			arg += fmt.Sprintf(" if @%d", i.Expected)
		}
		return fmt.Sprintf("%v(%s) -> %s", i.Kind, arg, res)
	},
	DescribeState: func(st any) string {
		s := st.(state)
		if !s.present {
			return "absent"
		}
		if !s.versionKnown {
			return fmt.Sprintf("%q@>%d", s.value, s.version)
		}
		return fmt.Sprintf("%q@%d", s.value, s.version)
	},
}

// History records operations from many clients. It is safe for
// concurrent use.
type History struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

// NewHistory starts an empty history.
func NewHistory() *History { return &History{start: time.Now()} }

func (h *History) now() int64 { return int64(time.Since(h.start)) }

// Begin notes an operation's invocation; call the returned function with
// its output when it returns.
func (h *History) Begin(client int, in Input) func(Output) {
	call := h.now()
	return func(out Output) {
		ret := h.now()
		h.mu.Lock()
		defer h.mu.Unlock()
		h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
	}
}

// Ops returns the recorded operations, with unknown outcomes returning
// at the end of the history.
func (h *History) Ops() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	end := h.now() + 1
	out := make([]porcupine.Operation, len(h.ops))
	for i, op := range h.ops {
		if op.Output.(Output).Unknown {
			op.Return = end
		}
		out[i] = op
	}
	return out
}

// Mark returns the history's clock, for KnownBetween.
func (h *History) Mark() int64 { return h.now() }

// KnownBetween counts the operations with a known outcome whose response
// arrived between two marks: reads, and writes (puts, deletes and
// compare-and-sets).
func (h *History) KnownBetween(from, to int64) (reads, writes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, op := range h.ops {
		if op.Output.(Output).Unknown || op.Return < from || op.Return > to {
			continue
		}
		if op.Input.(Input).Kind == Get {
			reads++
		} else {
			writes++
		}
	}
	return reads, writes
}

// Summary counts operations by kind and outcome.
func (h *History) Summary() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	counts := map[string]int{}
	for _, op := range h.ops {
		k := op.Input.(Input).Kind.String()
		if op.Output.(Output).Unknown {
			k += "?"
		}
		counts[k]++
	}
	var parts []string
	for _, k := range []string{"put", "put?", "get", "get?", "delete", "delete?", "cas", "cas?"} {
		if counts[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", k, counts[k]))
		}
	}
	return fmt.Sprintf("%d operations (%s)", len(h.ops), strings.Join(parts, ", "))
}

// Check checks the history; it returns porcupine.Ok, Illegal or Unknown
// (if checking took longer than timeout) and the linearization info for a
// visualization.
func (h *History) Check(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	return porcupine.CheckOperationsVerbose(Model.ToModel(), h.Ops(), timeout)
}
