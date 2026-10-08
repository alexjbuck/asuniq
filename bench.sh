#!/usr/bin/env bash
# Compare asuniq against huniq and the classic awk dedup one-liner.
# Usage: ./bench.sh [lines] [distinct]
set -euo pipefail

lines=${1:-5000000}
distinct=${2:-10000}
input=$(mktemp)
trap 'rm -f "$input"' EXIT

cargo build --release -q
awk -v n="$lines" -v k="$distinct" \
  'BEGIN { srand(1); for (i = 0; i < n; i++) print "some-long-line-with-an-item-" int(rand() * k) }' >"$input"

# asuniq is approximate (bounded LRU), so speed only matters alongside output size
echo "output lines (exact unique = $(sort -u "$input" | wc -l | tr -d ' ')):"
for cmd in ./target/release/asuniq huniq "awk '!seen[\$0]++'"; do
  printf '  %-22s %s\n' "$cmd" "$(eval "$cmd" <"$input" | wc -l | tr -d ' ')"
done

hyperfine --warmup 1 --input "$input" \
  -n asuniq ./target/release/asuniq \
  -n huniq huniq \
  -n awk "awk '!seen[\$0]++'"
