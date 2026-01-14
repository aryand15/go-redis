#!/bin/sh
set -e

echo ""
echo "--------------------------------------------------------------"
echo "Redis Implementation (go-redis) vs. Official Redis - Benchmark"
echo "--------------------------------------------------------------"
echo ""


REQUESTS=100000
CLIENTS=50
TESTS="GET,SET"

# This function takes two parameters:
#   1) the output of the redis-benchmark command
#   2) the name of the command to extract results for (GET, SET, etc.)
# It returns the requests per second & p99 latency for that command in the format "RPS|P99"
parse_results() {
    local command="$2"

    # Save to temp file for easier processing
    echo "$1" > /tmp/bench_output.txt

    # Extract the section for this command
    sed -n "/====== $command ======/,/======.*======/p" /tmp/bench_output.txt > /tmp/section.txt

    # Extract RPS
    local rps=$(grep "throughput summary:" /tmp/section.txt | awk '{print $3}')

    # Extract p99
    local p99=$(grep -A1 "avg.*min.*p50.*p95.*p99.*max" /tmp/section.txt | tail -1 | awk '{print $5}')

    # Clean up
    rm -f /tmp/section.txt /tmp/bench_output.txt

    # Return results
    if [ -n "$rps" ] && [ -n "$p99" ]; then
        echo "$rps|$p99"
    else
        echo "N/A|N/A"
    fi
}

echo ">>> Benchmarking go-redis implementation..."
echo ""

# Start go-redis server
./go-redis > /dev/null 2>&1 &
GO_REDIS_PID=$!
sleep 2

# Run redis-benchmark on go-redis
GO_REDIS_OUTPUT=$(redis-benchmark -p 6379 -n $REQUESTS -c $CLIENTS -t $TESTS 2>&1)

# Stop go-redis
kill $GO_REDIS_PID 2>/dev/null || true
wait $GO_REDIS_PID 2>/dev/null || true
sleep 1

echo ">>> Benchmarking official Redis server..."
echo ""

# Start official redis-server
redis-server --port 6379 --daemonize yes --save "" --appendonly no

# Run benchmark on official Redis
REDIS_OUTPUT=$(redis-benchmark -p 6379 -n $REQUESTS -c $CLIENTS -t $TESTS 2>&1)

# Stop redis-server
redis-cli -p 6379 shutdown NOSAVE 2>/dev/null || true
sleep 1

echo ""
echo "Results:"
echo ""

# Parse results for GET command
GO_GET_RESULTS=$(parse_results "$GO_REDIS_OUTPUT" "GET")
REDIS_GET_RESULTS=$(parse_results "$REDIS_OUTPUT" "GET")

GO_GET_RPS=$(echo "$GO_GET_RESULTS" | cut -d'|' -f1)
GO_GET_P99=$(echo "$GO_GET_RESULTS" | cut -d'|' -f2)
REDIS_GET_RPS=$(echo "$REDIS_GET_RESULTS" | cut -d'|' -f1)
REDIS_GET_P99=$(echo "$REDIS_GET_RESULTS" | cut -d'|' -f2)

# Parse results for SET command
GO_SET_RESULTS=$(parse_results "$GO_REDIS_OUTPUT" "SET")
REDIS_SET_RESULTS=$(parse_results "$REDIS_OUTPUT" "SET")

GO_SET_RPS=$(echo "$GO_SET_RESULTS" | cut -d'|' -f1)
GO_SET_P99=$(echo "$GO_SET_RESULTS" | cut -d'|' -f2)
REDIS_SET_RPS=$(echo "$REDIS_SET_RESULTS" | cut -d'|' -f1)
REDIS_SET_P99=$(echo "$REDIS_SET_RESULTS" | cut -d'|' -f2)

# Display results in a table format
printf "%-20s %-20s %-20s\n" "Metric" "go-redis" "Official Redis"
printf "%-20s %-20s %-20s\n" "--------------------" "--------------------" "--------------------"
printf "%-20s %-20s %-20s\n" "GET RPS" "$GO_GET_RPS" "$REDIS_GET_RPS"
printf "%-20s %-20s %-20s\n" "GET P99 (msec)" "$GO_GET_P99" "$REDIS_GET_P99"
printf "%-20s %-20s %-20s\n" "SET RPS" "$GO_SET_RPS" "$REDIS_SET_RPS"
printf "%-20s %-20s %-20s\n" "SET P99 (msec)" "$GO_SET_P99" "$REDIS_SET_P99"
echo ""

# Compare go-redis as a percentage of official Redis
echo ""
echo "Performance Comparison (go-redis as % of Official Redis):"
echo ""

# Calculate percentages for GET
if [ "$REDIS_GET_RPS" != "N/A" ] && [ "$GO_GET_RPS" != "N/A" ]; then
    GET_PERCENT=$(awk "BEGIN {printf \"%.1f\", ($GO_GET_RPS / $REDIS_GET_RPS) * 100}")
    printf "%-20s %s%%\n" "GET Throughput:" "$GET_PERCENT"
else
    printf "%-20s %s\n" "GET Throughput:" "N/A"
fi

# Calculate percentages for SET
if [ "$REDIS_SET_RPS" != "N/A" ] && [ "$GO_SET_RPS" != "N/A" ]; then
    SET_PERCENT=$(awk "BEGIN {printf \"%.1f\", ($GO_SET_RPS / $REDIS_SET_RPS) * 100}")
    printf "%-20s %s%%\n" "SET Throughput:" "$SET_PERCENT"
else
    printf "%-20s %s\n" "SET Throughput:" "N/A"
fi

echo ""