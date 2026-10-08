# asuniq - Approximate Streaming Unique

This CLI tool exists to handle the `| sort -u` task but for an unbounded input where you need
streaming outputs. The caveat is that this is only approximately unique, as true global uniqueness
over unbounded time and input space would require unbounded memory usage. `asuniq` is safe to run
forever (1) on unbounded input.

> It will dedupe as best it can but it won't crash your system.

(1) Technically not _forever_. Entries are timestamped in milliseconds since startup as a `u64`,
which runs out after about 580 million years. I think that counts as forever.

## Usage

```
asuniq [--ttl SECONDS] [CAPACITY] < input > output
```

`--ttl` lets a line be printed again once it has gone unseen for that many seconds. Without it,
lines are remembered until something else needs their slot.

`CAPACITY` is the number of distinct lines to remember. It defaults to 16384 and is rounded up to
the next power of two. Memory use is 32 bytes per unit of capacity, allocated once at startup:

| capacity  | memory |
| --------- | ------ |
| 16384     | 512 KB |
| 1048576   | 32 MB  |
| 4194304   | 128 MB |

Invalid arguments are an error (exit status 2) rather than falling back to defaults.

Install from a checkout with `cargo install --path .`.

## Behavior

Each line is printed the first time it is seen, byte for byte and in input order. Later copies are
dropped as long as the line is still remembered. The trailing `\n` is not part of the comparison,
but everything else is, so `a` and `a\r` are different lines.

With `--ttl`, a line expires once it has gone unseen for longer than the TTL, and its next copy is
printed again. Every copy refreshes the timer, including the ones that are dropped, so a line that
keeps showing up stays suppressed and only reprints after a quiet gap. For example, with
`--ttl 3600` a user who hits your app all day is logged once, and logged again only after an hour
away.

Output is flushed after every read from stdin, so lines show up as soon as they arrive when the
input is live (`tail -f | asuniq`). If the downstream reader closes the pipe (`asuniq | head`),
asuniq exits quietly with status 0.

### Eviction

asuniq stores a 64-bit hash of each line, not the line itself, in a fixed-size open addressing
table that is twice the capacity. Each entry also records when it was last seen, in milliseconds
since startup. The clock is read once per read from stdin, so lines that arrive together share a
timestamp.

A lookup scans up to 16 slots starting from the hash's home slot. If it finds the hash, its
timestamp is refreshed, and the line is a duplicate unless it was last seen longer than the TTL
ago. If it finds an empty slot, the line is new and is stored there. If all 16 slots are taken by
other lines, the least recently seen of those 16 is evicted to make room.

The TTL and the eviction order use the same timestamp, so expired lines are always the first to be
evicted. Evicting an expired line costs nothing, since it would have printed again anyway.

Some consequences:

- This is LRU within a 16-slot window, not across the whole table. A recently seen line can be
  evicted while older lines elsewhere survive, so an occasional duplicate gets through even when
  the table is far from full.
- Without `--ttl`, a line is only forgotten when something else needs its slot. On a quiet stream
  a line can stay suppressed indefinitely.
- Two different lines with the same 64-bit hash are treated as the same line. With millions of
  distinct lines the odds of any collision are around one in ten million.

### Tuning

Set capacity to about twice the number of distinct lines you expect to be active at once: within
one TTL if you use `--ttl`, or over the whole run if you don't. With a TTL, a stream whose distinct
lines keep growing forever can still be deduplicated exactly, as long as the set active within one
TTL fits. Measured without a TTL on 10M lines with about 2M distinct values:

| capacity | table load | extra duplicates emitted |
| -------- | ---------- | ------------------------ |
| 4194304  | 25%        | 1                        |
| 2097152  | 47%        | 475 (0.02%)              |
| 16384    | overloaded | 7.85M (most lines)       |

When the number of distinct lines far exceeds capacity, most lines get through, because they are
evicted before they repeat. Raising capacity fixes this at the cost of memory, and lookups do not
get slower as the table grows.

## Performance

`./bench.sh [lines] [distinct]` compares asuniq against `huniq` and `awk '!seen[$0]++'` using
`hyperfine`, and reports each tool's output line count next to the exact unique count.

Results on an Apple Silicon Mac:

| input                          | asuniq             | huniq   |
| ------------------------------ | ------------------ | ------- |
| 5M lines, 10k distinct         | 50 ms (default)    | 48 ms   |
| 10M lines, ~2M distinct        | 252 ms (cap 4M)    | 1125 ms |

huniq is exact and its memory grows with the input (74 MB on the second workload). asuniq used a
fixed 130 MB at capacity 4M, including on empty input. The awk one-liner is roughly 20x slower
than either.
