package sstable

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

// WriterOptions configures a Writer. Zero values pick the defaults.
type WriterOptions struct {
	// BlockSize is the target size of a data block. Default 4 KiB.
	BlockSize int
	// BloomBitsPerKey sets the filter's size and so its false-positive
	// rate. Default 10 (about 0.8%). Negative disables the filter.
	BloomBitsPerKey int
	// RestartInterval is the number of entries between restart points.
	// Default 16.
	RestartInterval int
}

func (o WriterOptions) withDefaults() WriterOptions {
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.BloomBitsPerKey == 0 {
		o.BloomBitsPerKey = 10
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = 16
	}
	return o
}

// Writer builds a table. Entries must be added in strictly increasing
// ikey.Compare order. The caller owns the file: it syncs and closes it after
// Finish.
type Writer struct {
	opts     WriterOptions
	w        *bufio.Writer
	offset   uint64
	data     blockBuilder
	index    blockBuilder
	filter   *filterBuilder
	lastKey  []byte // encoded
	keyBuf   []byte
	props    Properties
	finished bool
	err      error
}

// NewWriter returns a Writer writing to w.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	opts = opts.withDefaults()
	tw := &Writer{
		opts:  opts,
		w:     bufio.NewWriterSize(w, 64<<10),
		data:  blockBuilder{restartInterval: opts.RestartInterval},
		index: blockBuilder{restartInterval: 1},
	}
	if opts.BloomBitsPerKey > 0 {
		tw.filter = &filterBuilder{bitsPerKey: opts.BloomBitsPerKey}
		tw.props.FilterBitsPerKey = uint64(opts.BloomBitsPerKey)
	}
	return tw
}

// ErrOutOfOrder is returned by Add for a key that does not sort after the
// previous one.
var ErrOutOfOrder = errors.New("sstable: keys added out of order")

// Add appends an entry. Key and value are copied.
func (w *Writer) Add(k ikey.Key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return errors.New("sstable: Add after Finish")
	}
	w.keyBuf = encodeKey(w.keyBuf[:0], k)
	if w.props.NumEntries > 0 && compareEncoded(w.lastKey, w.keyBuf) >= 0 {
		return fmt.Errorf("%w: %s after %s", ErrOutOfOrder, k, decodeKey(w.lastKey))
	}
	if w.props.NumEntries == 0 {
		w.props.SmallestKey = bytes.Clone(k.UserKey)
		w.props.MinSeq, w.props.MaxSeq = k.Seq(), k.Seq()
	}
	w.props.LargestKey = append(w.props.LargestKey[:0], k.UserKey...)
	w.props.MinSeq = min(w.props.MinSeq, k.Seq())
	w.props.MaxSeq = max(w.props.MaxSeq, k.Seq())
	w.props.NumEntries++
	if k.Kind() == ikey.KindDelete {
		w.props.NumDeletions++
	}
	w.props.RawKeyBytes += uint64(len(k.UserKey))
	w.props.RawValueBytes += uint64(len(value))
	if w.filter != nil {
		w.filter.add(k.UserKey)
	}
	w.data.add(w.keyBuf, value)
	w.lastKey = append(w.lastKey[:0], w.keyBuf...)
	if w.data.estimatedSize() >= w.opts.BlockSize {
		w.flushData()
	}
	return w.err
}

func (w *Writer) flushData() {
	if w.data.empty() {
		return
	}
	h := w.writeBlock(w.data.finish())
	w.data.reset()
	w.index.add(w.lastKey, h.append(nil))
	w.props.NumDataBlocks++
}

func (w *Writer) writeBlock(payload []byte) blockHandle {
	h := blockHandle{Offset: w.offset, Size: uint64(len(payload))}
	if w.err != nil {
		return h
	}
	if _, err := w.w.Write(payload); err != nil {
		w.err = err
		return h
	}
	if _, err := w.w.Write(appendTrailer(nil, payload)); err != nil {
		w.err = err
		return h
	}
	w.offset += uint64(len(payload)) + blockTrailerLen
	return h
}

// EstimatedSize returns the bytes written so far plus the pending block.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.data.estimatedSize())
}

// NumEntries returns the number of entries added.
func (w *Writer) NumEntries() uint64 { return w.props.NumEntries }

// Finish writes the remaining blocks and the footer and flushes the
// buffered writer. It does not sync.
func (w *Writer) Finish() (Properties, error) {
	if w.finished {
		return w.props, errors.New("sstable: Finish called twice")
	}
	w.finished = true
	w.flushData()
	var filter []byte
	if w.filter != nil {
		filter = w.filter.finish()
	}
	fh := w.writeBlock(filter)
	ih := w.writeBlock(w.index.finish())
	ph := w.writeBlock(w.props.encode(nil))
	footer := fh.append(nil)
	footer = ih.append(footer)
	footer = ph.append(footer)
	footer = append(footer, byte(formatVersion), 0, 0, 0)
	footer = appendUint64(footer, tableMagic)
	if w.err == nil {
		if _, err := w.w.Write(footer); err != nil {
			w.err = err
		}
	}
	if w.err == nil {
		w.err = w.w.Flush()
	}
	if w.err != nil {
		return w.props, fmt.Errorf("sstable: write: %w", w.err)
	}
	w.offset += uint64(len(footer))
	return w.props, nil
}

// Size returns the file size after a successful Finish.
func (w *Writer) Size() uint64 { return w.offset }

func appendUint64(dst []byte, v uint64) []byte {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}
