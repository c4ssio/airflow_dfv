#!/usr/bin/env bash
# check_worker_memory.sh
# Monitor memory usage by process in the Airflow worker container
# Works without 'ps' command by reading /proc directly

set -euo pipefail

CONTAINER_NAME="airflow_dfv-airflow-worker-1"

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container '${CONTAINER_NAME}' is not running."
    echo "Available containers:"
    docker ps --format '{{.Names}}' | grep airflow || echo "No airflow containers found"
    exit 1
fi

echo "=== Memory Usage in ${CONTAINER_NAME} ==="
echo "Timestamp: $(date)"
echo ""

# Get container memory stats
echo "=== Container Memory Stats ==="
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" "${CONTAINER_NAME}"
echo ""

# Function to read process info from /proc (works without ps)
# /proc/[pid]/statm format: size resident shared text lib data dt
# We want field 1 (size) for VSZ and field 2 (resident) for RSS
read_proc_info() {
    docker exec "${CONTAINER_NAME}" sh -c '
        printf "%-8s %10s %10s %s\n" "PID" "RSS(MB)" "VSZ(MB)" "COMMAND"
        for pid in /proc/[0-9]*; do
            [ -d "$pid" ] || continue
            pidnum=$(basename "$pid")
            
            # Read statm (memory info)
            # statm format: size resident shared text lib data dt
            # field 1 = size (VSZ), field 2 = resident (RSS)
            if [ -r "$pid/statm" ]; then
                statm_line=$(cat "$pid/statm" 2>/dev/null)
                [ -z "$statm_line" ] && continue
                set -- $statm_line
                size_pages=$1
                rss_pages=$2
                rss_mb=$((rss_pages * 4096 / 1024 / 1024))
                vss_mb=$((size_pages * 4096 / 1024 / 1024))
            else
                continue
            fi
            
            # Read comm (command name)
            if [ -r "$pid/comm" ]; then
                comm=$(cat "$pid/comm" 2>/dev/null | tr -d "\n")
            else
                comm="unknown"
            fi
            
            # Read cmdline (full command)
            if [ -r "$pid/cmdline" ]; then
                cmdline=$(cat "$pid/cmdline" 2>/dev/null | tr "\0" " " | sed "s/ $//")
                [ -z "$cmdline" ] && cmdline="[$comm]"
            else
                cmdline="[$comm]"
            fi
            
            printf "%-8s %10d %10d %s\n" "$pidnum" "$rss_mb" "$vss_mb" "$cmdline"
        done | sort -k2 -rn | head -20
    '
}

# Top processes by memory usage (RSS)
echo "=== Top 20 Processes by Memory Usage (RSS) ==="
read_proc_info
echo ""

# Memory usage grouped by worker type
echo "=== Memory Usage by Worker Type ==="
docker exec "${CONTAINER_NAME}" sh <<'INNER_SCRIPT'
    for pid in /proc/[0-9]*; do
        [ -d "$pid" ] || continue
        
        # Read RSS from statm (field 2)
        if [ -r "$pid/statm" ]; then
            statm_line=$(cat "$pid/statm" 2>/dev/null)
            [ -z "$statm_line" ] && continue
            set -- $statm_line
            rss_pages=$2
            rss_kb=$((rss_pages * 4))
        else
            continue
        fi
        
        # Read cmdline to identify process type
        if [ -r "$pid/cmdline" ]; then
            cmdline=$(cat "$pid/cmdline" 2>/dev/null | tr "\0" " " | sed "s/ $//")
        else
            continue
        fi
        
        # Categorize process
        name="other"
        case "$cmdline" in
            *ForkPoolWorker*) name="ForkPoolWorker" ;;
            *MainProcess*) name="MainProcess" ;;
            *airflow*celery*worker*) name="celery-worker-main" ;;
            *airflow*serve-logs*) name="serve-logs" ;;
            *sh*) name="sh" ;;
            *dumb-init*) name="dumb-init" ;;
        esac
        
        echo "$name $rss_kb"
    done | awk '{
        name = $1
        rss = $2
        mem[name] += rss
        count[name]++
    }
    END {
        printf "%-30s %12s %10s %s\n", "PROCESS_TYPE", "TOTAL_RSS(MB)", "COUNT", "AVG_RSS(MB)"
        for (name in mem) {
            total_mb = mem[name] / 1024
            avg_mb = total_mb / count[name]
            printf "%-30s %12.1f %10d %10.1f\n", name, total_mb, count[name], avg_mb
        }
    }' | sort -k2 -rn
INNER_SCRIPT
echo ""

# Per-worker breakdown
echo "=== Per-Worker Memory Breakdown ==="
docker exec "${CONTAINER_NAME}" sh <<'INNER_SCRIPT'
    printf "%-8s %-25s %10s\n" "PID" "WORKER_NAME" "RSS(MB)"
    for pid in /proc/[0-9]*; do
        [ -d "$pid" ] || continue
        
        # Read cmdline
        if [ -r "$pid/cmdline" ]; then
            cmdline=$(cat "$pid/cmdline" 2>/dev/null | tr "\0" " " | sed "s/ $//")
        else
            continue
        fi
        
        # Check if it's a worker
        case "$cmdline" in
            *ForkPoolWorker*|*MainProcess*)
                # Extract worker name
                worker_name=$(echo "$cmdline" | sed 's/.*\[celeryd: //; s/\].*//')
                [ -z "$worker_name" ] && continue
                
                # Read RSS
                if [ -r "$pid/statm" ]; then
                    statm_line=$(cat "$pid/statm" 2>/dev/null)
                    [ -z "$statm_line" ] && continue
                    set -- $statm_line
                    rss_pages=$2
                    rss_mb=$((rss_pages * 4096 / 1024 / 1024))
                    
                    pidnum=$(basename "$pid")
                    printf "%-8s %-25s %10d\n" "$pidnum" "$worker_name" "$rss_mb"
                fi
                ;;
        esac
    done | sort -k3 -rn
INNER_SCRIPT
echo ""

# Python/Celery-specific processes
echo "=== Python/Celery Process Memory Details ==="
docker exec "${CONTAINER_NAME}" sh <<'INNER_SCRIPT'
    for pid in /proc/[0-9]*; do
        [ -d "$pid" ] || continue
        pidnum=$(basename "$pid")
        
        # Read command name and cmdline
        comm=""
        cmdline=""
        if [ -r "$pid/comm" ]; then
            comm=$(cat "$pid/comm" 2>/dev/null | tr -d "\n")
        fi
        if [ -r "$pid/cmdline" ]; then
            cmdline=$(cat "$pid/cmdline" 2>/dev/null | tr "\0" " " | sed "s/ $//")
        fi
        [ -z "$cmdline" ] && cmdline="[$comm]"
        
        # Check if it's a Python/Celery/Airflow process
        is_python_process=0
        case "$comm" in
            python*) is_python_process=1 ;;
            celery*) is_python_process=1 ;;
            airflow*) is_python_process=1 ;;
        esac
        case "$cmdline" in
            *python*) is_python_process=1 ;;
            *celery*) is_python_process=1 ;;
            *airflow*) is_python_process=1 ;;
        esac
        
        if [ "$is_python_process" = "1" ]; then
            # Read RSS from statm (field 2)
            if [ -r "$pid/statm" ]; then
                statm_line=$(cat "$pid/statm" 2>/dev/null)
                [ -z "$statm_line" ] && continue
                set -- $statm_line
                rss_pages=$2
                rss_mb=$((rss_pages * 4096 / 1024 / 1024))
            else
                continue
            fi
            
            printf "PID: %-8s RSS: %8d MB  CMD: %s\n" "$pidnum" "$rss_mb" "$cmdline"
        fi
    done | sort -k3 -rn
INNER_SCRIPT
echo ""

# System memory info
echo "=== Container System Memory Info ==="
docker exec "${CONTAINER_NAME}" sh -c 'cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable|Cached|Buffers"' || true
echo ""

# Cgroup memory stats
echo "=== Cgroup Memory Stats (if available) ==="
docker exec "${CONTAINER_NAME}" sh -c '
    if [ -f /sys/fs/cgroup/memory/memory.usage_in_bytes ]; then
        echo "Cgroup v1 memory usage:"
        cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null | awk "{printf \"Usage: %.1f MB\n\", \$1/1024/1024}" || true
        cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null | awk "{printf \"Limit: %.1f MB\n\", \$1/1024/1024}" || true
    elif [ -f /sys/fs/cgroup/memory.current ]; then
        echo "Cgroup v2 memory usage:"
        cat /sys/fs/cgroup/memory.current 2>/dev/null | awk "{printf \"Usage: %.1f MB\n\", \$1/1024/1024}" || true
        cat /sys/fs/cgroup/memory.max 2>/dev/null | awk "{printf \"Limit: %.1f MB\n\", \$1/1024/1024}" || true
    else
        echo "Cgroup memory stats not available"
    fi
' || true
echo ""

# Recommendations
echo "=== Recommendations ==="
echo "To identify memory leaks:"
echo "  1. Run: ./scripts/monitor_worker_memory.sh [interval] [count]"
echo "     Example: ./scripts/monitor_worker_memory.sh 10 12  (monitor for 2 minutes)"
echo ""
echo "  2. Look for workers with memory that grows over time"
echo ""
echo "  3. Check your DAG tasks for:"
echo "     - Large data structures kept in memory"
echo "     - Unclosed file handles or connections"
echo "     - Caching that grows unbounded"
echo "     - JSON parsing of very large files (consider streaming)"
echo ""
echo "  4. If workers consistently use high memory:"
echo "     - Reduce AIRFLOW__CELERY__WORKER_CONCURRENCY (default is often 16)"
echo "     - Consider using task-level memory limits"
echo "     - Review if tasks can be split into smaller chunks"

