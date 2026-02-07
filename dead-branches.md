# Dead Branches Archive

Summary of all deleted branches and their changes, preserved for reference before cleanup.

Deleted on: 2026-02-07

---

## Branches with unique (unmerged) changes

### `more_params` (6 months old)

**Purpose:** Added configurable concurrency controls (`--max-pending-requests`, `--max-backlog`, `--fetch-timeout-ms`, `--fetch-max-wait-ms`) and documentation about connection design and performance tuning.

**Commits:**
- `4be6e5f` - More parameters

**Files changed:** `kitt/src/main.rs`, `CONNECTION_DESIGN.md`, `PERFORMANCE_TUNING.md`, `THROUGHPUT_IMPROVEMENTS.md`, `examples/high_throughput_test.sh`

**Key snippets:**

New CLI args for fine-grained producer/consumer tuning:
```rust
/// Maximum number of concurrent produce requests per producer thread (higher = more throughput, more memory)
#[arg(long, default_value = "100")]
max_pending_requests: usize,

/// Maximum message backlog before applying backpressure
#[arg(long, default_value = "100000")]
max_backlog: u64,

/// Fetch request timeout in milliseconds
#[arg(long, default_value = "5000")]
fetch_timeout_ms: u64,

/// Broker wait time in milliseconds
#[arg(long, default_value = "100")]
fetch_max_wait_ms: i32,
```

Connection design rationale (from `CONNECTION_DESIGN.md`):
> Kitt uses a dedicated Kafka connection for each producer and consumer thread.
> Unlike HTTP, the Kafka protocol maintains connection-level state (correlation IDs,
> request ordering, connection-level buffering). Sharing connections would require
> complex synchronization. With one connection per thread: no locking overhead,
> no contention, CPU cache efficiency, simpler code.

**Status:** Superseded. The monolithic `main.rs` was refactored into modules. Some ideas (like `--max-pending-requests`) were not carried forward but could be revisited.

---

### `lame_sfx` (5 months old)

**Purpose:** Added a full audio synthesis module to `kitt_throbbler` for Knight Rider-inspired scanner sound effects using the `rodio` crate.

**Commits:**
- `9815cd7` - lame_sfx version

**Files changed:** `kitt_throbbler/src/audio.rs` (new, 402 lines), `kitt_throbbler/src/test_runner.rs` (new), `kitt_throbbler/src/lib.rs`, `kitt_throbbler/Cargo.toml`, examples, and more (+2362 lines total)

**Key snippets:**

Audio player with synthesized scanner beeps:
```rust
pub struct KittAudioPlayer {
    #[cfg(feature = "audio")]
    _stream: Option<OutputStream>,
    #[cfg(feature = "audio")]
    sink: Option<Sink>,
    enabled: bool,
}

/// Plays a Knight Rider-inspired beep pattern (non-blocking)
pub fn play_scan_beep(&self, position: usize, max_position: usize, direction: i32) {
    // ...
    // Turnaround effect - distinctive "ping" sound
    let turnaround_freq = if position == 0 { 420.0 } else { 380.0 };
    let ping = SineWave::new(turnaround_freq)
        .take_duration(Duration::from_millis(60))
        .amplify(0.20);

    // Regular scanner: ascending sweep moving right, descending left
    let base_freq = if direction > 0 {
        150.0 + (progress * 400.0) // 150Hz to 550Hz
    } else {
        550.0 - (progress * 400.0) // 550Hz to 150Hz
    };

    // Golden ratio harmonics for pleasant "electronic" quality
    let second_harmonic = base_freq * 1.618;
    let third_harmonic = base_freq * 2.414;
}
```

API change from `audio_enabled()` to `with_audio()` and thread-local audio player:
```rust
thread_local! {
    static AUDIO_PLAYER: RefCell<Option<audio::KittAudioPlayer>> = RefCell::new(None);
}
```

**Status:** Superseded. Main already has a different audio implementation using embedded MP3 playback (`sfx.mp3`) instead of synthesized tones. This branch's synthesized approach is an interesting alternative if the MP3 approach is ever dropped.

---

### `fetch-delay` (5 months old)

**Purpose:** Added `--fetch-delay` flag to introduce a configurable delay before consumers start fetching, with automatic backlog threshold adjustment for accurate throughput measurement.

**Commits:**
- `81adc2f` - feat: add --fetch-delay with backlog compensation (default: 1s)
- `eda0ddb` - increase default delay to 5s

**Files changed:** `kitt/src/main.rs`, `README.md`

**Key snippets:**

Backlog compensation formula:
```rust
const BASE_MAX_BACKLOG: u64 = 1000;

// Calculate adjusted max backlog to compensate for fetch delay
let max_backlog = if args.fetch_delay > 0 {
    BASE_MAX_BACKLOG * args.fetch_delay
} else {
    BASE_MAX_BACKLOG
};
```

Consumer delay before fetching:
```rust
// Apply fetch delay if configured
if self.fetch_delay > 0 {
    info!("Consumer thread {} waiting {}s before starting",
        self.thread_id, self.fetch_delay);
    tokio::time::sleep(Duration::from_secs(self.fetch_delay)).await;
}
```

**Status:** Fully merged into main. The feature exists in `kitt/src/args.rs`, `kitt/src/consumer.rs`, `kitt/src/measurer.rs`, and `kitt-core/`.

---

### `verify` (5 months old)

**Purpose:** Verification/testing branch that added partition-level queue profiling with per-partition timing statistics and detailed fetch diagnostics.

**Commits:**
- `a2c9a66` - verify branch

**Files changed:** `kitt/src/main.rs` (+423 lines)

**Key snippets:**

The branch contained an older monolithic version of `main.rs` with added queue-level profiling (per-partition fetch timing, detailed response diagnostics). These capabilities were later refactored into the modular architecture.

**Status:** Superseded by the current modular codebase.

---

### `queue-profiling` (5 months old)

**Purpose:** Merge branch combining `fetch-delay` and `verify` features -- consumer startup delay + per-partition queue profiling.

**Commits:**
- `1f7c50f` - Merge branch 'verify': Combine fetch_delay and sticky partitioning features
- Plus all commits from `fetch-delay` and `verify`

**Status:** Superseded. Both constituent features have been integrated into main through the modular refactor.

---

## Branches with no unique changes (already in main)

| Branch | Age | Last Commit Message |
|--------|-----|-------------------|
| `correct` | 6 months | Fixed idempotent producer issues and initial retry behavior |
| `parallel_partitions` | 6 months | Fixed idempotent producer issues and initial retry behavior |
| `random_produce` | 6 months | Fixed idempotent producer issues and initial retry behavior |
| `delay_again` | 5 months | Funny topic names |
| `origin/big-refa` | 13 days | feat: add --quiet mode for machine-readable output |
| `origin/delay_again` | 5 months | Fix author field in cargo |

### `origin/big-refa` (remote only)

The major refactoring branch that extracted `main.rs` into modules (`args.rs`, `consumer.rs`, `producer.rs`, `measurer.rs`), added `--quiet` mode, clippy fixes, and readability improvements. All commits merged into main.
