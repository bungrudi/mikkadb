#!/bin/bash

SAMPLE_FILE="benchmark_results/sample-output.txt"
OUTPUT_FILE="benchmark_results/allocation-stats.txt"

echo "=== Allocation Hotspot Analysis ===" > "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Extract total samples
TOTAL_SAMPLES=$(grep -E "^\s+[0-9]+ Thread" "$SAMPLE_FILE" | head -1 | awk '{print $1}')
echo "Total Samples: $TOTAL_SAMPLES" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Count allocation-related samples
echo "Counting allocation-related function calls..." >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Malloc calls
MALLOC_COUNT=$(grep -o "malloc" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "malloc calls: $MALLOC_COUNT" >> "$OUTPUT_FILE"

# Free calls
FREE_COUNT=$(grep -o "_free\|free" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "free calls: $FREE_COUNT" >> "$OUTPUT_FILE"

# Rust allocation calls
RUST_ALLOC=$(grep -o "__rust_alloc\|___rust_alloc" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "Rust __rust_alloc: $RUST_ALLOC" >> "$OUTPUT_FILE"

# RawVec grow (Vec allocations)
RAWVEC_GROW=$(grep -o "RawVec.*grow" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "RawVec::grow (Vec allocations): $RAWVEC_GROW" >> "$OUTPUT_FILE"

echo "" >> "$OUTPUT_FILE"
echo "=== Top Allocation Hotspots ===" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Extract lines with malloc/alloc and their sample counts
grep -E "malloc|alloc|RawVec|grow_one" "$SAMPLE_FILE" | \
  grep -E "^\s+\+" | \
  sed 's/^[[:space:]]*//' | \
  sort | uniq -c | sort -rn | head -20 >> "$OUTPUT_FILE"

echo "" >> "$OUTPUT_FILE"
echo "=== Function Call Breakdown ===" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Count samples for key functions
echo "Analyzing key function call frequencies..." >> "$OUTPUT_FILE"

# I/O operations
IO_SAMPLES=$(grep -E "sendto|recvfrom|__send|__recv" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "I/O operations (__sendto/__recvfrom): $IO_SAMPLES samples" >> "$OUTPUT_FILE"

# Lock operations
LOCK_SAMPLES=$(grep -E "pthread_mutex|pthread_cond|RwLock" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "Lock operations (pthread_mutex/cond/RwLock): $LOCK_SAMPLES samples" >> "$OUTPUT_FILE"

# RESP parsing
RESP_SAMPLES=$(grep -E "parse_message|parse_integer" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "RESP parsing (parse_message/parse_integer): $RESP_SAMPLES samples" >> "$OUTPUT_FILE"

# String operations
STRING_SAMPLES=$(grep -E "from_utf8|String::from|to_uppercase" "$SAMPLE_FILE" | wc -l | tr -d ' ')
echo "String operations (from_utf8/String::from/to_uppercase): $STRING_SAMPLES samples" >> "$OUTPUT_FILE"

echo "" >> "$OUTPUT_FILE"
echo "=== Estimated CPU Time Breakdown ===" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Calculate rough percentages (this is approximate)
if [ "$TOTAL_SAMPLES" -gt 0 ]; then
    IO_PCT=$(awk "BEGIN {printf \"%.1f\", ($IO_SAMPLES / $TOTAL_SAMPLES) * 100}")
    LOCK_PCT=$(awk "BEGIN {printf \"%.1f\", ($LOCK_SAMPLES / $TOTAL_SAMPLES) * 100}")
    RESP_PCT=$(awk "BEGIN {printf \"%.1f\", ($RESP_SAMPLES / $TOTAL_SAMPLES) * 100}")
    STRING_PCT=$(awk "BEGIN {printf \"%.1f\", ($STRING_SAMPLES / $TOTAL_SAMPLES) * 100}")

    echo "I/O operations: ~$IO_PCT%" >> "$OUTPUT_FILE"
    echo "Lock operations: ~$LOCK_PCT%" >> "$OUTPUT_FILE"
    echo "RESP parsing: ~$RESP_PCT%" >> "$OUTPUT_FILE"
    echo "String operations: ~$STRING_PCT%" >> "$OUTPUT_FILE"
fi

echo "" >> "$OUTPUT_FILE"
echo "Analysis complete. See $OUTPUT_FILE for detailed results."
cat "$OUTPUT_FILE"
