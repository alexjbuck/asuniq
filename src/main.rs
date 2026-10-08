use std::env;
use std::io::{self, BufWriter, Read, Write};

// A single flat slot in our contiguous memory block
#[derive(Clone, Copy, Default)]
struct CacheSlot {
    hash: u64,
    last_seen: u64, // Monotonically increasing counter for LRU tracking
}

struct ArenaCache {
    slots: Vec<CacheSlot>,
    mask: usize, // Used for lightning-fast modulo if capacity is a power of 2
    counter: u64,
}

impl ArenaCache {
    fn new(mut capacity: usize) -> Self {
        // Ensure capacity is a power of 2 for fast bitwise masking instead of '%'
        if !capacity.is_power_of_two() {
            capacity = capacity.next_power_of_two();
        }
        Self {
            slots: vec![CacheSlot::default(); capacity * 2], // 50% load factor for minimal collisions
            mask: (capacity * 2) - 1,
            counter: 0,
        }
    }

    // Returns true if seen, false if new. Handles LRU eviction automatically.
    fn check_and_update(&mut self, hash: u64) -> bool {
        self.counter += 1;
        // High-bit mixing to map the 64-bit hash into our slot index
        let mut idx = (hash as usize) & self.mask;

        let mut lowest_counter = u64::MAX;
        let mut eviction_idx = idx;
        let mut probes = 0;

        // Linear probing loop - incredibly friendly to CPU L1/L2 cache lines
        loop {
            let slot = &mut self.slots[idx];

            // Scenario 1: Empty slot found. This is a brand new unique item.
            if slot.hash == 0 {
                slot.hash = hash;
                slot.last_seen = self.counter;
                return false;
            }

            // Scenario 2: Cache hit. Update its recency counter.
            if slot.hash == hash {
                slot.last_seen = self.counter;
                return true;
            }

            // Keep track of the oldest item we encounter while probing in case we need to evict
            if slot.last_seen < lowest_counter {
                lowest_counter = slot.last_seen;
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
                evict_slot.last_seen = self.counter;
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
    #[inline(always)]
    fn hash(bytes: &[u8]) -> u64 {
        // Bypasses std::hash object traits completely for maximum speed
        rapidhash::v3::rapidhash_v3_inline::<true, false, false>(
            bytes,
            &rapidhash::v3::DEFAULT_RAPID_SECRETS,
        )
    }
}

type ChosenHasher = RapidHasher;

fn main() -> io::Result<()> {
    // A closed downstream pipe (e.g. `asuniq | head`) is a normal way to stop, not an error
    match run() {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => result,
    }
}

fn run() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let capacity: usize = if args.len() > 1 {
        args[1].parse().unwrap_or(16384)
    } else {
        16384
    };

    let mut cache = ArenaCache::new(capacity);
    dedup(
        io::stdin().lock(),
        io::stdout().lock(),
        &mut cache,
        CHUNK_SIZE,
    )
}

const CHUNK_SIZE: usize = 64 * 1024;

// Reads big chunks and walks every '\n' in one SIMD memchr pass, instead of a
// fresh search and a copy for every (often tiny) line
fn dedup(
    mut reader: impl Read,
    writer: impl Write,
    cache: &mut ArenaCache,
    chunk_size: usize,
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
        let mut start = 0;
        for end in memchr::memchr_iter(b'\n', &buf[filled..filled + n]) {
            let end = filled + end + 1;
            emit_if_new(&buf[start..end], cache, &mut writer)?;
            start = end;
        }
        filled += n;
        buf.copy_within(start..filled, 0);
        filled -= start;

        // Flush once per read, not per line: output stays live when input stalls (e.g. `tail -f`)
        writer.flush()?;
    }

    if filled > 0 {
        emit_if_new(&buf[..filled], cache, &mut writer)?;
    }
    writer.flush()
}

// `line` includes its trailing '\n' if it has one; the hash ignores it
fn emit_if_new(line: &[u8], cache: &mut ArenaCache, writer: &mut impl Write) -> io::Result<()> {
    let key = line.strip_suffix(b"\n").unwrap_or(line);
    if !cache.check_and_update(ChosenHasher::hash(key)) {
        writer.write_all(line)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

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
        // With room for every distinct line, the cache must never evict, so output is exact
        // regardless of how reads split the input
        #[test]
        fn exact_when_under_capacity(
            input in prop::collection::vec(prop::sample::select(b"ab\r\n".to_vec()), 0..400),
            chunk_size in 1usize..64,
        ) {
            let mut cache = ArenaCache::new(512);
            let mut output = Vec::new();
            dedup(&input[..], &mut output, &mut cache, chunk_size).unwrap();
            prop_assert_eq!(output, exact_unique(&input));
        }
    }
}
