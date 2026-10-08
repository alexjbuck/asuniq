use std::env;
use std::io::{self, BufWriter, Read, Write};
use std::process::ExitCode;
use std::time::Instant;

// A single flat slot in our contiguous memory block
#[derive(Clone, Copy, Default)]
struct CacheSlot {
    hash: u64,
    last_seen: u64, // Milliseconds since startup, drives both LRU and TTL
}

struct ArenaCache {
    slots: Vec<CacheSlot>,
    mask: usize, // Used for lightning-fast modulo if capacity is a power of 2
    ttl: u64,    // Milliseconds; u64::MAX means lines never expire
}

impl ArenaCache {
    fn new(mut capacity: usize, ttl: u64) -> Self {
        // Ensure capacity is a power of 2 for fast bitwise masking instead of '%'
        if !capacity.is_power_of_two() {
            capacity = capacity.next_power_of_two();
        }
        Self {
            slots: vec![CacheSlot::default(); capacity * 2], // 50% load factor for minimal collisions
            mask: (capacity * 2) - 1,
            ttl,
        }
    }

    // Returns true if seen within the TTL, false if new or stale. Handles LRU eviction automatically.
    fn check_and_update(&mut self, hash: u64, now: u64) -> bool {
        // High-bit mixing to map the 64-bit hash into our slot index
        #[expect(
            clippy::cast_possible_truncation,
            reason = "only the low bits survive the mask anyway"
        )]
        let mut idx = (hash as usize) & self.mask;

        let mut oldest_seen = u64::MAX;
        let mut eviction_idx = idx;
        let mut probes = 0;

        // Linear probing loop - incredibly friendly to CPU L1/L2 cache lines
        loop {
            let slot = &mut self.slots[idx];

            // Scenario 1: Empty slot found. This is a brand new unique item.
            if slot.hash == 0 {
                slot.hash = hash;
                slot.last_seen = now;
                return false;
            }

            // Scenario 2: Cache hit. Only a duplicate if it was seen within the TTL, but either
            // way it's active again, so refresh it.
            if slot.hash == hash {
                let fresh = now.saturating_sub(slot.last_seen) <= self.ttl;
                slot.last_seen = now;
                return fresh;
            }

            // Keep track of the oldest item we encounter while probing in case we need to evict.
            // Expired items are always the oldest, so they go first.
            if slot.last_seen < oldest_seen {
                oldest_seen = slot.last_seen;
                eviction_idx = idx;
            }

            // Move to the next contiguous memory slot (Linear Probing)
            idx = (idx + 1) & self.mask;
            probes += 1;

            // If we've probed a small window (e.g., 16 slots) without finding it,
            // we evict the Least Recently Used item in that cache line window to bound latency.
            if probes == 16 {
                let evict_slot = &mut self.slots[eviction_idx];
                evict_slot.hash = hash;
                evict_slot.last_seen = now;
                return false;
            }
        }
    }
}

// Define a unified contract for our streaming hashers
trait StreamHasher {
    fn hash(bytes: &[u8]) -> u64;
}

// Implement Strategy 1: Rapidhash v3 Inline Engine
struct RapidHasher;
impl StreamHasher for RapidHasher {
    #[inline]
    fn hash(bytes: &[u8]) -> u64 {
        // Bypasses std::hash object traits completely for maximum speed
        rapidhash::v3::rapidhash_v3_inline::<true, false, false>(
            bytes,
            &rapidhash::v3::DEFAULT_RAPID_SECRETS,
        )
    }
}

type ChosenHasher = RapidHasher;

const USAGE: &str = "usage: asuniq [--ttl SECONDS] [CAPACITY]";

fn main() -> ExitCode {
    let (capacity, ttl) = match parse_args(env::args().skip(1)) {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("asuniq: {msg}\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let mut cache = ArenaCache::new(capacity, ttl);
    let start = Instant::now();
    let clock = move || u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    match dedup(
        io::stdin().lock(),
        io::stdout().lock(),
        &mut cache,
        CHUNK_SIZE,
        clock,
    ) {
        Ok(()) => ExitCode::SUCCESS,
        // A closed downstream pipe (e.g. `asuniq | head`) is a normal way to stop, not an error
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("asuniq: {e}");
            ExitCode::FAILURE
        }
    }
}

// Returns (capacity, ttl in milliseconds). Without --ttl, lines never expire.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<(usize, u64), String> {
    let mut capacity = 16384;
    let mut ttl = u64::MAX;
    while let Some(arg) = args.next() {
        if arg == "--ttl" {
            let secs: u64 = args
                .next()
                .and_then(|s| s.parse().ok())
                .ok_or("--ttl needs a whole number of seconds")?;
            ttl = secs.saturating_mul(1000);
        } else {
            capacity = arg
                .parse()
                .map_err(|_| format!("invalid capacity: {arg}"))?;
        }
    }
    Ok((capacity, ttl))
}

const CHUNK_SIZE: usize = 64 * 1024;

// Reads big chunks and walks every '\n' in one SIMD memchr pass, instead of a
// fresh search and a copy for every (often tiny) line.
// `clock` is read once per chunk: under load that's microseconds apart, and on a live
// stream each read is when the data arrived anyway.
fn dedup(
    mut reader: impl Read,
    writer: impl Write,
    cache: &mut ArenaCache,
    chunk_size: usize,
    mut clock: impl FnMut() -> u64,
) -> io::Result<()> {
    let mut writer = BufWriter::new(writer);
    let mut buf = vec![0; chunk_size];
    // buf[..filled] is a partial line carried over from the previous read (never contains '\n')
    let mut filled = 0;

    loop {
        // A single line longer than the buffer: grow until it fits
        if filled == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }
        let n = match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        // Only the new bytes need scanning, the carried-over prefix has no '\n'
        let now = clock();
        let mut start = 0;
        for end in memchr::memchr_iter(b'\n', &buf[filled..filled + n]) {
            let end = filled + end + 1;
            emit_if_new(&buf[start..end], cache, now, &mut writer)?;
            start = end;
        }
        filled += n;
        buf.copy_within(start..filled, 0);
        filled -= start;

        // Flush once per read, not per line: output stays live when input stalls (e.g. `tail -f`)
        writer.flush()?;
    }

    if filled > 0 {
        emit_if_new(&buf[..filled], cache, clock(), &mut writer)?;
    }
    writer.flush()
}

// `line` includes its trailing '\n' if it has one; the hash ignores it
fn emit_if_new(
    line: &[u8],
    cache: &mut ArenaCache,
    now: u64,
    writer: &mut impl Write,
) -> io::Result<()> {
    let key = line.strip_suffix(b"\n").unwrap_or(line);
    if !cache.check_and_update(ChosenHasher::hash(key), now) {
        writer.write_all(line)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

    // Exact first-occurrence dedup, splitting lines the same way `dedup` does
    fn exact_unique(input: &[u8]) -> Vec<u8> {
        let mut seen = HashSet::new();
        input
            .split_inclusive(|&b| b == b'\n')
            .filter(|line| seen.insert(line.strip_suffix(b"\n").unwrap_or(line)))
            .flatten()
            .copied()
            .collect()
    }

    proptest! {
        // With room for every distinct line and no TTL, the cache must never forget, so output is
        // exact regardless of how reads split the input or how much time passes
        #[test]
        fn exact_when_under_capacity(
            input in prop::collection::vec(prop::sample::select(b"ab\r\n".to_vec()), 0..400),
            chunk_size in 1usize..64,
        ) {
            let mut cache = ArenaCache::new(512, u64::MAX);
            let mut output = Vec::new();
            let mut now = 0;
            let clock = || { now += 1000; now };
            dedup(&input[..], &mut output, &mut cache, chunk_size, clock).unwrap();
            prop_assert_eq!(output, exact_unique(&input));
        }

        // Under capacity, a line is a duplicate iff it was last seen no more than `ttl` ago
        #[test]
        fn ttl_since_last_seen(
            events in prop::collection::vec((0u8..20, 0u64..10), 0..400),
            ttl in 0u64..20,
        ) {
            let mut cache = ArenaCache::new(512, ttl);
            let mut last_seen = HashMap::new();
            let mut now = 0;
            for (line, gap) in events {
                now += gap;
                let expected = last_seen.insert(line, now).is_some_and(|prev| now - prev <= ttl);
                prop_assert_eq!(cache.check_and_update(ChosenHasher::hash(&[line]), now), expected);
            }
        }
    }

    // Each 2-byte read is one whole line, and the clock advances 10ms per read
    #[test]
    fn dedup_reads_clock_per_chunk() {
        let run = |ttl| {
            let mut cache = ArenaCache::new(16, ttl);
            let mut output = Vec::new();
            let mut now = 0;
            let clock = || {
                now += 10;
                now
            };
            dedup(&b"a\na\na\n"[..], &mut output, &mut cache, 2, clock).unwrap();
            output
        };
        assert_eq!(run(5), b"a\na\na\n");
        assert_eq!(run(50), b"a\n");
    }

    #[test]
    fn parse_args_rejects_garbage() {
        let parse = |args: &[&str]| parse_args(args.iter().map(ToString::to_string));
        assert_eq!(parse(&[]), Ok((16384, u64::MAX)));
        assert_eq!(parse(&["--ttl", "60", "1000"]), Ok((1000, 60_000)));
        assert!(parse(&["1e6"]).is_err());
        assert!(parse(&["--ttl"]).is_err());
        assert!(parse(&["--ttl", "1.5"]).is_err());
    }
}
