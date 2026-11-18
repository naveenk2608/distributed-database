package lsm

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/naveenk2608/distributed-database/internal/storage/sstable"
	"github.com/naveenk2608/distributed-database/internal/storage/vfs"
)

// SyncPolicy says when the engine fsyncs the log. See docs/PLAN.md, "Sync
// policy".
type SyncPolicy int

const (
	// SyncAlways fsyncs before acknowledging a write. Concurrent writes
	// share one fsync through group commit. This is the default and the
	// only policy that never loses an acknowledged write.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs every Options.SyncInterval. A power loss can lose
	// writes acknowledged in the last interval; standalone mode only.
	SyncInterval
	// SyncNever leaves fsync to the operating system (and to Close). It
	// exists to measure what fsync costs.
	SyncNever
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncInterval:
		return "interval"
	case SyncNever:
		return "never"
	default:
		return fmt.Sprintf("SyncPolicy(%d)", int(p))
	}
}

// Options configures Open. The zero value is valid.
type Options struct {
	// FS is the filesystem; nil means the operating system's.
	FS vfs.FS
	// MemtableBytes is the size at which the active memtable becomes
	// immutable, a new log segment starts, and a flush is scheduled.
	// 0 means 4 MiB.
	MemtableBytes int64
	// Sync is the log sync policy.
	Sync SyncPolicy
	// DisableWAL keeps no write-ahead log: writes become durable only when
	// their memtable is flushed (Flush, or a full memtable), and a crash or
	// Close loses the rest; LastSeq after Open is the last flushed sequence
	// number. For an owner that logs every write itself and replays it
	// after a restart, such as a replica, whose Raft log is the only
	// write-ahead log (docs/adr/0010). Log segments left by an earlier run
	// with a log are replayed and flushed at Open. Sync is then unused.
	DisableWAL bool
	// SyncInterval is the fsync period for SyncInterval. 0 means 10ms.
	SyncInterval time.Duration
	// Logger receives recovery and background messages; nil discards.
	Logger *slog.Logger

	// BlockSize is the SSTable data block size. 0 means 4 KiB.
	BlockSize int
	// BloomBitsPerKey sizes each table's bloom filter. 0 means 10 (about
	// 0.8% false positives); negative disables filters.
	BloomBitsPerKey int
	// BlockCache caches data blocks; share one across every engine on a
	// node. nil gives this engine a private 8 MiB cache.
	BlockCache *sstable.Cache

	// MaxImmutableMemtables is how many full memtables may wait for a flush
	// before writes stall. 0 means 2.
	MaxImmutableMemtables int
	// CompactionMinTables is the smallest number of similar-sized adjacent
	// tables that triggers a compaction. 0 means 4.
	CompactionMinTables int
	// CompactionMaxTables caps the tables merged by one compaction.
	// 0 means 10.
	CompactionMaxTables int
	// CompactionSizeRatio is the largest ratio between the biggest and the
	// smallest table that still counts as "similar size". 0 means 2.
	CompactionSizeRatio float64
	// SlowdownTables is the table count at which each write group is
	// delayed by 1ms to let compaction catch up. 0 means 20.
	SlowdownTables int
	// StopTables is the table count at which writes wait for compaction
	// before starting a new memtable. 0 means 30.
	StopTables int
	// CompactionBytesPerSec limits compaction write bandwidth; 0 means
	// unlimited.
	CompactionBytesPerSec int64
	// CompactionPool limits how many engines compact at once; share one
	// across a node. nil means this engine compacts on its own.
	CompactionPool *CompactionPool

	// manual disables the background flush and compaction goroutines and
	// write stalls; tests then drive flushes and compactions themselves so
	// every filesystem operation happens in a reproducible order.
	manual bool
}

func (o Options) withDefaults() Options {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 4 << 20
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 10 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.BloomBitsPerKey == 0 {
		o.BloomBitsPerKey = 10
	}
	if o.BlockCache == nil {
		o.BlockCache = sstable.NewCache(8 << 20)
	}
	if o.MaxImmutableMemtables <= 0 {
		o.MaxImmutableMemtables = 2
	}
	if o.CompactionMinTables <= 1 {
		o.CompactionMinTables = 4
	}
	if o.CompactionMaxTables < o.CompactionMinTables {
		o.CompactionMaxTables = max(10, o.CompactionMinTables)
	}
	if o.CompactionSizeRatio < 1 {
		o.CompactionSizeRatio = 2
	}
	if o.SlowdownTables <= 0 {
		o.SlowdownTables = 20
	}
	if o.StopTables <= o.SlowdownTables {
		o.StopTables = o.SlowdownTables + 10
	}
	return o
}

// CompactionPool bounds concurrent compactions across engines.
type CompactionPool struct {
	slots chan struct{}
}

// NewCompactionPool returns a pool allowing n concurrent compactions.
func NewCompactionPool(n int) *CompactionPool {
	return &CompactionPool{slots: make(chan struct{}, max(n, 1))}
}
