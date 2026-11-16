package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

// Reader reads a table. It keeps the index, filter and properties in memory
// and reads data blocks through the cache. It is safe for concurrent use.
// The caller owns the file and must keep it open while the Reader is used.
type Reader struct {
	f       io.ReaderAt
	size    int64
	name    string // for error messages
	cache   *Cache
	cacheID uint64

	index  []byte
	filter []byte
	props  Properties

	filterChecks   atomic.Int64
	filterExcluded atomic.Int64
}

// ReaderOptions configures Open.
type ReaderOptions struct {
	// Cache, if non-nil, caches data blocks.
	Cache *Cache
	// Name labels errors, usually the file name.
	Name string
}

// Open reads and verifies the footer, index, filter and properties of a
// table of the given size.
func Open(f io.ReaderAt, size int64, opts ReaderOptions) (*Reader, error) {
	r := &Reader{f: f, size: size, name: opts.Name, cache: opts.Cache, cacheID: nextCacheID.Add(1)}
	if size < footerLen {
		return nil, r.wrap(corruptf("file of %d bytes is shorter than the footer", size))
	}
	footer := make([]byte, footerLen)
	if _, err := f.ReadAt(footer, size-footerLen); err != nil {
		return nil, r.wrap(fmt.Errorf("read footer: %w", err))
	}
	if binary.LittleEndian.Uint64(footer[footerLen-8:]) != tableMagic {
		return nil, r.wrap(corruptf("bad magic number"))
	}
	if v := footer[3*handleLen]; v != formatVersion {
		return nil, r.wrap(corruptf("unsupported format version %d", v))
	}
	fh := decodeHandle(footer)
	ih := decodeHandle(footer[handleLen:])
	ph := decodeHandle(footer[2*handleLen:])
	var err error
	if r.filter, err = r.readRaw(fh); err != nil {
		return nil, r.wrap(fmt.Errorf("filter block: %w", err))
	}
	if r.index, err = r.readRaw(ih); err != nil {
		return nil, r.wrap(fmt.Errorf("index block: %w", err))
	}
	if _, err := newBlockIter(r.index, compareEncoded); err != nil {
		return nil, r.wrap(fmt.Errorf("index block: %w", err))
	}
	pb, err := r.readRaw(ph)
	if err != nil {
		return nil, r.wrap(fmt.Errorf("properties block: %w", err))
	}
	if r.props, err = decodeProperties(pb); err != nil {
		return nil, r.wrap(err)
	}
	return r, nil
}

func (r *Reader) wrap(err error) error {
	if r.name == "" {
		return err
	}
	return fmt.Errorf("%s: %w", r.name, err)
}

// readRaw reads and verifies the block at h without the cache.
func (r *Reader) readRaw(h blockHandle) ([]byte, error) {
	end := h.Offset + h.Size + blockTrailerLen
	if end < h.Offset || end > uint64(r.size)-footerLen {
		return nil, corruptf("block handle [%d, %d) outside the table", h.Offset, end)
	}
	buf := make([]byte, h.Size+blockTrailerLen)
	if _, err := r.f.ReadAt(buf, int64(h.Offset)); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return checkBlock(buf)
}

func (r *Reader) readBlock(h blockHandle) ([]byte, error) {
	k := cacheKey{table: r.cacheID, offset: h.Offset}
	if b, ok := r.cache.get(k); ok {
		return b, nil
	}
	b, err := r.readRaw(h)
	if err != nil {
		return nil, r.wrap(fmt.Errorf("data block at %d: %w", h.Offset, err))
	}
	r.cache.put(k, b)
	return b, nil
}

// Properties returns the table's properties.
func (r *Reader) Properties() Properties { return r.props }

// FilterStats returns how many lookups consulted the filter and how many
// of those the filter ruled out.
func (r *Reader) FilterStats() (checks, excluded int64) {
	return r.filterChecks.Load(), r.filterExcluded.Load()
}

// EvictFromCache drops this table's blocks from the cache, for when the
// table is deleted.
func (r *Reader) EvictFromCache() { r.cache.evictTable(r.cacheID) }

// MayContain reports whether the filter allows userKey to be in the table.
func (r *Reader) MayContain(userKey []byte) bool {
	if len(r.filter) == 0 {
		return true
	}
	r.filterChecks.Add(1)
	if filterMayContain(r.filter, userKey) {
		return true
	}
	r.filterExcluded.Add(1)
	return false
}

// Get returns the newest entry for userKey with sequence number <= seq.
// found is false if the table has none. The value aliases cached memory and
// must not be modified.
func (r *Reader) Get(userKey []byte, seq uint64) (value []byte, trailer ikey.Trailer, found bool, err error) {
	if bytes.Compare(userKey, r.props.SmallestKey) < 0 || bytes.Compare(userKey, r.props.LargestKey) > 0 ||
		r.props.NumEntries == 0 || seq < r.props.MinSeq || !r.MayContain(userKey) {
		return nil, 0, false, nil
	}
	it := r.NewIter()
	it.SeekGE(ikey.SeekKey(userKey, seq))
	if err := it.Err(); err != nil {
		return nil, 0, false, err
	}
	if !it.Valid() || !bytes.Equal(it.Key().UserKey, userKey) {
		return nil, 0, false, nil
	}
	return it.Value(), it.Key().Trailer, true, nil
}

// Iter iterates over a table's entries in order. It implements the engine's
// internal iterator: First, SeekGE, Next, Valid, Key, Value, Err.
type Iter struct {
	r      *Reader
	index  *blockIter
	data   *blockIter
	target []byte
	err    error
}

// NewIter returns an unpositioned iterator.
func (r *Reader) NewIter() *Iter {
	idx, err := newBlockIter(r.index, compareEncoded) // verified in Open
	return &Iter{r: r, index: idx, err: err}
}

func (it *Iter) loadBlock() bool {
	h, n := decodeHandleVarint(it.index.Value())
	if n <= 0 {
		it.err = it.r.wrap(corruptf("bad block handle in index"))
		return false
	}
	b, err := it.r.readBlock(h)
	if err != nil {
		it.err = err
		return false
	}
	if it.data == nil {
		it.data, err = newBlockIter(b, compareEncoded)
	} else {
		err = it.data.init(b)
	}
	if err != nil {
		it.err = it.r.wrap(err)
		it.data = nil
		return false
	}
	return true
}

// skipEmpty moves to the next block while the current block is exhausted.
func (it *Iter) skipEmpty() {
	for it.err == nil && (it.data == nil || !it.data.Valid()) {
		if it.data != nil {
			if err := it.data.Err(); err != nil {
				it.err = it.r.wrap(err)
				return
			}
		}
		if !it.index.Valid() {
			it.data = nil
			return
		}
		it.index.Next()
		if !it.index.Valid() {
			if err := it.index.Err(); err != nil {
				it.err = it.r.wrap(err)
			}
			it.data = nil
			return
		}
		if !it.loadBlock() {
			return
		}
		it.data.First()
	}
}

func (it *Iter) First() {
	if it.err != nil {
		return
	}
	it.index.First()
	if !it.index.Valid() {
		it.data = nil
		return
	}
	if !it.loadBlock() {
		return
	}
	it.data.First()
	it.skipEmpty()
}

func (it *Iter) SeekGE(target ikey.Key) {
	if it.err != nil {
		return
	}
	it.target = encodeKey(it.target[:0], target)
	it.index.SeekGE(it.target)
	if !it.index.Valid() {
		if err := it.index.Err(); err != nil {
			it.err = it.r.wrap(err)
		}
		it.data = nil
		return
	}
	if !it.loadBlock() {
		return
	}
	it.data.SeekGE(it.target)
	it.skipEmpty()
}

func (it *Iter) Next() {
	if it.err != nil || it.data == nil {
		return
	}
	it.data.Next()
	it.skipEmpty()
}

func (it *Iter) Valid() bool { return it.err == nil && it.data != nil && it.data.Valid() }

// Key returns the current internal key; it is valid until the next move.
func (it *Iter) Key() ikey.Key { return decodeKey(it.data.Key()) }

func (it *Iter) Value() []byte { return it.data.Value() }

// Err returns the first error the iterator hit, such as a corrupt block.
func (it *Iter) Err() error { return it.err }

func decodeHandleVarint(b []byte) (blockHandle, int) {
	if len(b) != handleLen {
		return blockHandle{}, -1
	}
	return decodeHandle(b), handleLen
}
