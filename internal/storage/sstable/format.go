// Package sstable implements dkv's immutable sorted table files.
//
//	[data block 0] ... [data block N]   ~BlockSize each: prefix-compressed keys + restart points
//	[filter block]                      bloom filter over user keys
//	[index block]                       last key of each data block -> block handle
//	[properties block]                  key range, sequence range, counts
//	[footer]                            handles to filter/index/properties, version, magic
//	every block := payload | compression(1) | crc32c(4)
//
// Keys are internal keys (ikey) encoded as userKey | trailer(8, little
// endian); entries are in ikey.Compare order. Every block carries a CRC-32C
// over its payload and compression byte, so torn writes and bit rot are
// detected when a block is read. docs/storage-format.md has the byte-level
// layout. The package does no file management: the engine creates, syncs
// and removes the files.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/naveenk2608/distributed-database/internal/storage/ikey"
)

const (
	blockTrailerLen = 5 // compression(1) + crc32c(4)
	handleLen       = 16
	footerLen       = 3*handleLen + 4 + 8
	formatVersion   = 1
	tableMagic      = uint64(0x31747373766b64) // "dkvsst1" little-endian
	compressionNone = 0
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means a table failed a checksum or structural check.
var ErrCorrupt = errors.New("sstable: corrupt table")

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// blockHandle locates a block; Size excludes the trailer.
type blockHandle struct {
	Offset, Size uint64
}

func (h blockHandle) append(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, h.Offset)
	return binary.LittleEndian.AppendUint64(dst, h.Size)
}

func decodeHandle(b []byte) blockHandle {
	return blockHandle{binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])}
}

// encodeKey appends the table encoding of k: user key, then the 8-byte
// trailer.
func encodeKey(dst []byte, k ikey.Key) []byte {
	dst = append(dst, k.UserKey...)
	return binary.LittleEndian.AppendUint64(dst, uint64(k.Trailer))
}

// decodeKey splits an encoded key. The result aliases b. Callers have
// already checked len(b) >= 8.
func decodeKey(b []byte) ikey.Key {
	n := len(b) - 8
	return ikey.Key{UserKey: b[:n:n], Trailer: ikey.Trailer(binary.LittleEndian.Uint64(b[n:]))}
}

// compareEncoded orders encoded keys like ikey.Compare.
func compareEncoded(a, b []byte) int {
	return ikey.Compare(decodeKey(a), decodeKey(b))
}

// Properties describe a table. They are stored in the table and returned by
// Writer.Finish and Reader.Properties.
type Properties struct {
	SmallestKey, LargestKey []byte // user keys
	MinSeq, MaxSeq          uint64
	NumEntries              uint64
	NumDeletions            uint64
	RawKeyBytes             uint64
	RawValueBytes           uint64
	NumDataBlocks           uint64
	FilterBitsPerKey        uint64
}

func (p *Properties) encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(p.SmallestKey)))
	dst = append(dst, p.SmallestKey...)
	dst = binary.AppendUvarint(dst, uint64(len(p.LargestKey)))
	dst = append(dst, p.LargestKey...)
	for _, v := range []uint64{p.MinSeq, p.MaxSeq, p.NumEntries, p.NumDeletions,
		p.RawKeyBytes, p.RawValueBytes, p.NumDataBlocks, p.FilterBitsPerKey} {
		dst = binary.AppendUvarint(dst, v)
	}
	return dst
}

func decodeProperties(b []byte) (Properties, error) {
	var p Properties
	readBytes := func() ([]byte, bool) {
		n, k := binary.Uvarint(b)
		if k <= 0 || n > uint64(len(b)-k) {
			return nil, false
		}
		out := b[k : k+int(n) : k+int(n)]
		b = b[k+int(n):]
		return out, true
	}
	var ok bool
	if p.SmallestKey, ok = readBytes(); !ok {
		return p, corruptf("bad properties: smallest key")
	}
	if p.LargestKey, ok = readBytes(); !ok {
		return p, corruptf("bad properties: largest key")
	}
	for _, v := range []*uint64{&p.MinSeq, &p.MaxSeq, &p.NumEntries, &p.NumDeletions,
		&p.RawKeyBytes, &p.RawValueBytes, &p.NumDataBlocks, &p.FilterBitsPerKey} {
		n, k := binary.Uvarint(b)
		if k <= 0 {
			return p, corruptf("bad properties: truncated counters")
		}
		*v = n
		b = b[k:]
	}
	if len(b) != 0 {
		return p, corruptf("bad properties: %d trailing bytes", len(b))
	}
	return p, nil
}

// appendTrailer appends the compression byte and the CRC of payload plus
// that byte.
func appendTrailer(dst, payload []byte) []byte {
	dst = append(dst, compressionNone)
	crc := crc32.Update(crc32.Checksum(payload, castagnoli), castagnoli, []byte{compressionNone})
	return binary.LittleEndian.AppendUint32(dst, crc)
}

// checkBlock verifies a block read with its trailer and returns the payload.
func checkBlock(b []byte) ([]byte, error) {
	if len(b) < blockTrailerLen {
		return nil, corruptf("block of %d bytes is shorter than its trailer", len(b))
	}
	n := len(b) - blockTrailerLen
	payload, comp := b[:n], b[n]
	if comp != compressionNone {
		return nil, corruptf("unknown compression %d", comp)
	}
	if crc32.Checksum(b[:n+1], castagnoli) != binary.LittleEndian.Uint32(b[n+1:]) {
		return nil, corruptf("block checksum mismatch")
	}
	return payload, nil
}
