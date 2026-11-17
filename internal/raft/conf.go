package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// ConfState is a group's membership: voters elect leaders and count
// toward quorums; learners only receive the log.
type ConfState struct {
	Voters   []string // sorted, distinct
	Learners []string // sorted, distinct, none of them voters
}

// IsVoter reports whether id votes.
func (c ConfState) IsVoter(id string) bool { return slices.Contains(c.Voters, id) }

// IsLearner reports whether id is a learner.
func (c ConfState) IsLearner(id string) bool { return slices.Contains(c.Learners, id) }

// IsMember reports whether id is a voter or a learner.
func (c ConfState) IsMember(id string) bool { return c.IsVoter(id) || c.IsLearner(id) }

// Members returns voters and learners.
func (c ConfState) Members() []string {
	out := slices.Concat(c.Voters, c.Learners)
	slices.Sort(out)
	return out
}

// Clone returns a deep copy.
func (c ConfState) Clone() ConfState {
	return ConfState{Voters: slices.Clone(c.Voters), Learners: slices.Clone(c.Learners)}
}

// Equal reports whether c and o list the same members.
func (c ConfState) Equal(o ConfState) bool {
	return slices.Equal(c.Voters, o.Voters) && slices.Equal(c.Learners, o.Learners)
}

func (c ConfState) String() string { return fmt.Sprintf("voters %v learners %v", c.Voters, c.Learners) }

// ConfChangeType is a single-server membership change.
type ConfChangeType uint8

const (
	// ConfBootstrap is the first entry of a new group: its initial voters.
	ConfBootstrap ConfChangeType = iota + 1
	// ConfAddLearner adds a node that receives the log but does not vote.
	ConfAddLearner
	// ConfPromote makes a learner a voter.
	ConfPromote
	// ConfRemove removes a voter or a learner.
	ConfRemove
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfBootstrap:
		return "bootstrap"
	case ConfAddLearner:
		return "add-learner"
	case ConfPromote:
		return "promote"
	case ConfRemove:
		return "remove"
	default:
		return fmt.Sprintf("ConfChangeType(%d)", uint8(t))
	}
}

// ConfChange is the payload of an EntryConfChange: the change and the
// membership that results from it. Carrying the result lets every node
// adopt it without replaying earlier changes.
type ConfChange struct {
	Type ConfChangeType
	Node string
	Conf ConfState
}

// ErrInvalidConfChange reports a change that does not fit the current
// membership.
var ErrInvalidConfChange = errors.New("raft: invalid membership change")

// apply returns the membership after applying a change of type t for node.
func (c ConfState) apply(t ConfChangeType, node string) (ConfState, error) {
	n := c.Clone()
	switch t {
	case ConfAddLearner:
		if c.IsMember(node) {
			return c, fmt.Errorf("%w: %s is already a member", ErrInvalidConfChange, node)
		}
		n.Learners = append(n.Learners, node)
		slices.Sort(n.Learners)
	case ConfPromote:
		if !c.IsLearner(node) {
			return c, fmt.Errorf("%w: %s is not a learner", ErrInvalidConfChange, node)
		}
		n.Learners = slices.DeleteFunc(n.Learners, func(s string) bool { return s == node })
		n.Voters = append(n.Voters, node)
		slices.Sort(n.Voters)
	case ConfRemove:
		switch {
		case c.IsVoter(node):
			if len(c.Voters) == 1 {
				return c, fmt.Errorf("%w: cannot remove the last voter", ErrInvalidConfChange)
			}
			n.Voters = slices.DeleteFunc(n.Voters, func(s string) bool { return s == node })
		case c.IsLearner(node):
			n.Learners = slices.DeleteFunc(n.Learners, func(s string) bool { return s == node })
		default:
			return c, fmt.Errorf("%w: %s is not a member", ErrInvalidConfChange, node)
		}
	default:
		return c, fmt.Errorf("%w: type %v", ErrInvalidConfChange, t)
	}
	return n, nil
}

// EncodeConfChange encodes cc for an entry's data.
func EncodeConfChange(cc ConfChange) []byte {
	b := []byte{byte(cc.Type)}
	b = appendString(b, cc.Node)
	b = appendStrings(b, cc.Conf.Voters)
	return appendStrings(b, cc.Conf.Learners)
}

// DecodeConfChange decodes an EntryConfChange's data.
func DecodeConfChange(b []byte) (ConfChange, error) {
	var cc ConfChange
	if len(b) == 0 {
		return cc, errors.New("raft: empty membership change")
	}
	cc.Type, b = ConfChangeType(b[0]), b[1:]
	var err error
	if cc.Node, b, err = readString(b); err != nil {
		return cc, err
	}
	if cc.Conf.Voters, b, err = readStrings(b); err != nil {
		return cc, err
	}
	if cc.Conf.Learners, b, err = readStrings(b); err != nil {
		return cc, err
	}
	if len(b) != 0 {
		return cc, errors.New("raft: trailing bytes after a membership change")
	}
	return cc, nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendStrings(b []byte, ss []string) []byte {
	b = binary.AppendUvarint(b, uint64(len(ss)))
	for _, s := range ss {
		b = appendString(b, s)
	}
	return b
}

var errShortConf = errors.New("raft: truncated membership change")

func readString(b []byte) (string, []byte, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < n {
		return "", nil, errShortConf
	}
	return string(b[k : k+int(n)]), b[k+int(n):], nil
}

func readStrings(b []byte) ([]string, []byte, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil, nil, errShortConf
	}
	b = b[k:]
	var out []string
	for range n {
		var s string
		var err error
		if s, b, err = readString(b); err != nil {
			return nil, nil, err
		}
		out = append(out, s)
	}
	return out, b, nil
}

// SnapshotMeta describes a state machine snapshot: it reflects every
// entry up to Index, whose term is Term, and Conf is the membership at
// Index.
type SnapshotMeta struct {
	Index, Term uint64
	Conf        ConfState
}

// IsEmpty reports whether s describes no snapshot.
func (s SnapshotMeta) IsEmpty() bool { return s.Index == 0 }
