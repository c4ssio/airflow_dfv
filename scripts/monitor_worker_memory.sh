#!/usr/bin/env bash
# monitor_worker_memory.sh
# Monitor memory usage over time to identify leaks
# Usage: ./scripts/monitor_worker_memory.sh [interval_seconds] [count]

set -euo pipefail

CONTAINER_NAME="airflow_dfv-airflow-worker-1"
INTERVAL=${1:-10}  # Default 10 seconds
COUNT=${2:-6}      # Default 6 samples (1 minute at 10s intervals)

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container '${CONTAINER_NAME}' is not running."
    exit 1
fi

OUTPUT_FILE="diagnostics/memory_timeline_$(date +%Y%m%d_%H%M%S).txt"
mkdir -p diagnostics

echo "Monitoring memory usage in ${CONTAINER_NAME}"
echo "Interval: ${INTERVAL}s, Samples: ${COUNT}"
echo "Output: ${OUTPUT_FILE}"
echo ""

# Function to get worker memory stats
get_worker_stats() {
    docker exec "${CONTAINER_NAME}" sh -c '
        timestamp=$(date +%s)
        echo "TIMESTAMP=$timestamp"
        
        # Get all worker PIDs and their memory
        for pid in /proc/[0-9]*; do
            [ -d "$pid" ] || continue
            pidnum=$(basename "$pid")
            
            # Read cmdline to identify workers
            if [ -r "$pid/cmdline" ]; then
                cmdline=$(cat "$pid/cmdline" 2>/dev/null | tr "\0" " " | sed "s/ $//")
            else
                continue
            fi
            
            # Check if it'\''s a worker process
            case "$cmdline" in
                *ForkPoolWorker*|*MainProcess*|*celery*)
                    # Read RSS from statm (field 2)
                    if [ -r "$pid/statm" ]; then
                        statm_line=$(cat "$pid/statm" 2>/dev/null)
                        [ -z "$statm_line" ] && continue
                        set -- $statm_line
                        rss_pages=$2
                        rss_mb=$((rss_pages * 4096 / 1024 / 1024))
                        
                        # Extract worker name - handle different formats
                        if echo "$cmdline" | grep -q "ForkPoolWorker"; then
                            worker_name=$(echo "$cmdline" | sed "s/.*ForkPoolWorker-\\([0-9]*\\).*/ForkPoolWorker-\\1/")
                        elif echo "$cmdline" | grep -q "MainProcess"; then
                            worker_name="MainProcess"
                        else
                            worker_name="celery-worker"
                        fi
                        
                        echo "WORKER=$pidnum:$worker_name:$rss_mb"
                    fi
                    ;;
            esac
        done
        
        # Get container total
        if [ -r /sys/fs/cgroup/memory.current ]; then
            total_mb=$(cat /sys/fs/cgroup/memory.current 2>/dev/null | awk "{printf \"%.1f\", \$1/1024/1024}")
            echo "TOTAL=$total_mb"
        fi
    '
}

# Collect samples - write all data to temp file for processing
TEMP_STATS="/tmp/worker_memory_stats_$$.txt"
> "$TEMP_STATS"

echo "Collecting ${COUNT} samples..."
for ((i=1; i<=COUNT; i++)); do
    printf "Sample %d/%d... " "$i" "$COUNT"
    
    # Get stats - capture both stdout and stderr
    if ! stats=$(get_worker_stats 2>&1); then
        echo "ERROR: get_worker_stats function failed"
        echo "ERROR: get_worker_stats function failed" >> "$OUTPUT_FILE"
        [ $i -lt $COUNT ] && sleep "$INTERVAL"
        continue
    fi
    
    # Check if we got any output
    if [ -z "$stats" ]; then
        echo "ERROR: No stats returned (empty output)"
        echo "ERROR: No stats returned (empty output)" >> "$OUTPUT_FILE"
        [ $i -lt $COUNT ] && sleep "$INTERVAL"
        continue
    fi
    
    # Check if we got at least a timestamp
    if ! echo "$stats" | grep -q "^TIMESTAMP="; then
        echo "ERROR: Invalid stats format (no TIMESTAMP found)"
        echo "ERROR: Invalid stats format. First 200 chars: ${stats:0:200}" >> "$OUTPUT_FILE"
        [ $i -lt $COUNT ] && sleep "$INTERVAL"
        continue
    fi
    
    # Process stats and write to output
    {
        echo "=== Sample $i at $(date) ==="
        
        # Process workers - avoid subshell issues by using a different approach
        echo "$stats" | grep "^WORKER=" > "/tmp/workers_$$.txt" || true
        
        if [ -s "/tmp/workers_$$.txt" ]; then
            while read -r line; do
                # Parse WORKER=pid:name:mem format
                # Remove WORKER= prefix
                line="${line#WORKER=}"
                # Extract PID (first field before colon)
                pid="${line%%:*}"
                # Extract memory (last field after last colon)
                mem="${line##*:}"
                # Extract name (everything between first and last colon)
                name="${line#*:}"
                name="${name%:*}"
                
                echo "  PID $pid ($name): ${mem} MB"
                echo "$name:$mem" >> "$TEMP_STATS"
            done < "/tmp/workers_$$.txt"
            rm -f "/tmp/workers_$$.txt"
        else
            echo "  (no workers found)"
        fi
        
        total=$(echo "$stats" | grep "^TOTAL=" | cut -d= -f2)
        [ -n "$total" ] && echo "  Container Total: ${total} MB"
        echo ""
    } | tee -a "$OUTPUT_FILE"
    
    echo "done"
    [ $i -lt $COUNT ] && sleep "$INTERVAL"
done

# Summary - process temp file with awk (no associative arrays needed)
echo ""
echo "=== Summary ===" | tee -a "$OUTPUT_FILE"
echo "Worker Memory Statistics:" | tee -a "$OUTPUT_FILE"
printf "%-25s %10s %10s %10s %10s\n" "WORKER_TYPE" "AVG(MB)" "MIN(MB)" "MAX(MB)" "SAMPLES" | tee -a "$OUTPUT_FILE"
if [ -s "$TEMP_STATS" ]; then
    awk -F: '{
        name = $1
        mem = $2
        if (min[name] == "" || mem < min[name]) min[name] = mem
        if (max[name] == "" || mem > max[name]) max[name] = mem
        total[name] += mem
        count[name]++
    }
    END {
        for (name in total) {
            avg = int(total[name] / count[name])
            printf "%-25s %10d %10d %10d %10d\n", name, avg, min[name], max[name], count[name]
        }
    }' "$TEMP_STATS" | sort -k2 -rn | tee -a "$OUTPUT_FILE"
else
    echo "No worker data collected" | tee -a "$OUTPUT_FILE"
fi
rm -f "$TEMP_STATS"

echo ""
echo "Analysis:" | tee -a "$OUTPUT_FILE"
echo "- If MAX >> MIN for a worker type, that suggests memory growth during task execution" | tee -a "$OUTPUT_FILE"
echo "- If workers consistently grow over time, check for memory leaks in your tasks" | tee -a "$OUTPUT_FILE"
echo "- Consider reducing worker pool size if memory usage is high" | tee -a "$OUTPUT_FILE"
echo ""
echo "Full timeline saved to: $OUTPUT_FILE"

