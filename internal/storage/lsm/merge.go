package lsm

import "github.com/naveenk2608/distributed-database/internal/storage/ikey"

// internalIterator walks internal keys in ikey.Compare order. Memtable
// iterators implement it; SSTable iterators will too.
type internalIterator interface {
	First()
	SeekGE(target ikey.Key)
	Next()
	Valid() bool
	Key() ikey.Key
	Value() []byte
	// Err returns the first error the iterator hit; an iterator that hit
	// one is no longer valid.
	Err() error
}

// mergingIter merges several internal iterators into one ordered stream with
// a binary min-heap. Inputs never share an internal key: every batch has its
// own sequence number and lives in exactly one source.
type mergingIter struct {
	iters []internalIterator
	heap  []internalIterator // the valid inputs, smallest key at heap[0]
}

func newMergingIter(iters ...internalIterator) *mergingIter {
	return &mergingIter{iters: iters, heap: make([]internalIterator, 0, len(iters))}
}

func (m *mergingIter) First() {
	for _, it := range m.iters {
		it.First()
	}
	m.init()
}

func (m *mergingIter) SeekGE(target ikey.Key) {
	for _, it := range m.iters {
		it.SeekGE(target)
	}
	m.init()
}

func (m *mergingIter) Next() {
	top := m.heap[0]
	top.Next()
	if !top.Valid() {
		last := len(m.heap) - 1
		m.heap[0] = m.heap[last]
		m.heap[last] = nil
		m.heap = m.heap[:last]
	}
	if len(m.heap) > 0 {
		m.down(0)
	}
}

// Err returns the first error of any input. An input that fails drops out
// of the merge, so callers must check Err before trusting the result.
func (m *mergingIter) Err() error {
	for _, it := range m.iters {
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m *mergingIter) Valid() bool   { return len(m.heap) > 0 }
func (m *mergingIter) Key() ikey.Key { return m.heap[0].Key() }
func (m *mergingIter) Value() []byte { return m.heap[0].Value() }

func (m *mergingIter) init() {
	clear(m.heap)
	m.heap = m.heap[:0]
	for _, it := range m.iters {
		if it.Valid() {
			m.heap = append(m.heap, it)
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
}

func (m *mergingIter) less(i, j int) bool {
	return ikey.Compare(m.heap[i].Key(), m.heap[j].Key()) < 0
}

func (m *mergingIter) down(i int) {
	n := len(m.heap)
	for {
		smallest := i
		if l := 2*i + 1; l < n && m.less(l, smallest) {
			smallest = l
		}
		if r := 2*i + 2; r < n && m.less(r, smallest) {
			smallest = r
		}
		if smallest == i {
			return
		}
		m.heap[i], m.heap[smallest] = m.heap[smallest], m.heap[i]
		i = smallest
	}
}
