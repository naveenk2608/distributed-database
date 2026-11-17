package linearizability

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

type op struct {
	call, ret int64
	in        Input
	out       Output
}

func check(ops []op) porcupine.CheckResult {
	var h []porcupine.Operation
	for i, o := range ops {
		h = append(h, porcupine.Operation{ClientId: i, Input: o.in, Call: o.call, Output: o.out, Return: o.ret})
	}
	return porcupine.CheckOperationsTimeout(Model.ToModel(), h, 0)
}

func put(k, v string) Input           { return Input{Kind: Put, Key: k, Value: v} }
func get(k string) Input              { return Input{Kind: Get, Key: k} }
func del(k string) Input              { return Input{Kind: Delete, Key: k} }
func cas(k, v string, e uint64) Input { return Input{Kind: CAS, Key: k, Value: v, Expected: e} }
func ver(v uint64) Output             { return Output{Version: v} }
func found(v string, at uint64) Output {
	return Output{Found: true, Value: v, Version: at}
}

var unknown = Output{Unknown: true}

const end = 1000 // an unknown outcome returns at the end of the history

func TestModelAcceptsLinearizableHistories(t *testing.T) {
	for name, h := range map[string][]op{
		"sequential": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, get("a"), found("x", 5)},
			{4, 5, cas("a", "y", 5), Output{Succeeded: true, Version: 7}},
			{6, 7, cas("a", "z", 5), Output{Succeeded: false, Version: 7}},
			{8, 9, get("a"), found("y", 7)},
			{10, 11, del("a"), Output{Found: true, Version: 9}},
			{12, 13, get("a"), Output{}},
			{14, 15, cas("a", "w", 0), Output{Succeeded: true, Version: 12}},
		},
		"concurrent put and get": {
			{0, 10, put("a", "x"), ver(5)},
			{1, 2, get("a"), Output{}}, // before the put took effect
			{3, 4, get("a"), found("x", 5)},
		},
		"unknown put may have happened": {
			{0, end, put("a", "x"), unknown},
			{5, 6, get("a"), Output{}},
			{7, 8, get("a"), found("x", 9)},
		},
		"unknown put may not have happened": {
			{0, end, put("a", "x"), unknown},
			{5, 6, get("a"), Output{}},
		},
		"unknown cas may have applied": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, cas("a", "y", 5), unknown},
			{3, 4, get("a"), found("y", 8)},
		},
		"failed cas learns an unknown version": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, put("a", "y"), unknown},
			{3, 4, cas("a", "z", 5), Output{Succeeded: false, Version: 9}},
			{5, 6, cas("a", "z", 9), Output{Succeeded: true, Version: 11}},
		},
		"cas creates a deleted key": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, del("a"), unknown},
			{3, 4, cas("a", "y", 0), Output{Succeeded: true, Version: 9}},
			{5, 6, get("a"), found("y", 9)},
		},
		"keys are independent": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, put("b", "y"), ver(3)},
			{4, 5, get("b"), found("y", 3)},
		},
	} {
		if got := check(h); got != porcupine.Ok {
			t.Errorf("%s: %v, want ok", name, got)
		}
	}
}

func TestModelRejectsViolations(t *testing.T) {
	for name, h := range map[string][]op{
		"stale read": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, put("a", "y"), ver(7)},
			{4, 5, get("a"), found("x", 5)},
		},
		"lost acknowledged write": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, get("a"), Output{}},
		},
		"compare-and-set applied twice": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, cas("a", "y", 5), Output{Succeeded: true, Version: 7}},
			{4, 5, cas("a", "z", 5), Output{Succeeded: true, Version: 9}},
		},
		"version goes backwards": {
			{0, 1, put("a", "x"), ver(7)},
			{2, 3, put("a", "y"), ver(5)},
		},
		"read of a version never written": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, get("a"), found("x", 6)},
		},
		"delete of a missing key reports found": {
			{0, 1, del("a"), Output{Found: true, Version: 3}},
		},
		"unknown put read at a version below the bound": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, put("a", "y"), unknown},
			{3, 4, get("a"), found("y", 4)},
		},
		"write after an unknown write goes backwards": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, put("a", "y"), unknown},
			{3, 4, put("a", "z"), ver(3)},
		},
		"unknown cas applied to an absent key at a nonzero version": {
			{0, 1, put("a", "x"), ver(5)},
			{2, end, del("a"), unknown},
			{3, end, cas("a", "y", 9), unknown},
			{4, 5, get("a"), found("y", 12)},
		},
		"failed cas on an absent key reports a version": {
			{0, 1, cas("a", "y", 3), Output{Succeeded: false, Version: 2}},
		},
		"cas succeeds at a version at or below the one it expected": {
			{0, end, put("a", "x"), unknown},
			{1, 2, cas("a", "y", 6), Output{Succeeded: true, Version: 6}},
		},
		"failed cas reports a wrong version": {
			{0, 1, put("a", "x"), ver(5)},
			{2, 3, cas("a", "y", 4), Output{Succeeded: false, Version: 6}},
		},
	} {
		if got := check(h); got != porcupine.Illegal {
			t.Errorf("%s: %v, want illegal", name, got)
		}
	}
}
