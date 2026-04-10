#!/usr/bin/env python3
"""
Experiment 3: WriteBufferManager Write-Stall Behavior
DS614 Final Project — RocksDB MemTable Layer

Measures how write_buffer_size affects flush frequency and the latency
profile of write operations, with particular focus on the stall threshold
defined in write_buffer_manager.cc.

Source code reference (write_buffer_manager.cc):

  ShouldFlush():
    Triggers flush when memory_active_ >= 90% of buffer_size_
    OR when memory_active_ > mutable_limit_ (= buffer_size_ * 7/8 = 87.5%).

  ReserveMem(bytes):
    Called by AllocTracker::Allocate() (alloc_tracker.cc) every time
    the memtable arena allocates memory.
    Updates memory_used_ and memory_active_ atomically.

  MaybeEndWriteStall() / BeginWriteStall():
    When allow_stall=true, writers are suspended via a condition variable
    queue when ShouldStall() returns true (memory_used_ > buffer_size_).

  Stall threshold (from write_buffer_manager_test.cc):
    A 10MB buffer triggers ShouldFlush() at ~9MB (90%),
    which matches: mutable_limit_ = buffer_size_ * 7/8

Experiment design:
  Write 500K key-value pairs using python-rocksdb with varying
  write_buffer_size: [4MB, 8MB, 16MB, 32MB, 64MB].
  Per write_buffer_size, we record:
    - Total elapsed time
    - Estimated flush count = total_bytes / write_buffer_size
    - Per-batch latency to detect stall signatures (latency spikes)
    - Throughput (ops/sec and MB/s)

  Expected result:
    Smaller buffers → more frequent flushes → more latency spikes and
    lower sustained throughput due to compaction I/O and flush overhead.

Requires:
  pip install python-rocksdb matplotlib numpy psutil

  If python-rocksdb is unavailable (not all platforms support it),
  the script falls back to a simulation mode that models the 90% threshold
  behaviour from the source and prints a note explaining the fallback.
"""

import os
import sys
import csv
import time
import random
import shutil
import tempfile

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "../../results/exp3"))
PLOTS_DIR   = os.path.normpath(os.path.join(SCRIPT_DIR, "../../results/plots"))

# ── Config ────────────────────────────────────────────────────────────────────
NUM_WRITES       = 500_000
VALUE_SIZE       = 100          # bytes per value
KEY_SIZE         = 16           # bytes per key  (zero-padded int)
BATCH_SIZE       = 1_000        # keys per WriteBatch (reduces Python overhead)
SPIKE_THRESHOLD  = 3.0          # latency spike detection: > N × median

# write_buffer_size values to sweep (bytes)
BUFFER_SIZES = [
    4   * 1024 * 1024,   #  4 MB
    8   * 1024 * 1024,   #  8 MB
    16  * 1024 * 1024,   # 16 MB
    32  * 1024 * 1024,   # 32 MB
    64  * 1024 * 1024,   # 64 MB
    128 * 1024 * 1024,   # 128 MB
]

VALUE_BYTES = b"v" * VALUE_SIZE


def prepare_plot_env() -> None:
    """Use writable temp caches so matplotlib works in restricted environments."""
    cache_root = os.path.join(tempfile.gettempdir(), "rocksdb_memtable_study_plot_cache")
    paths = {
        "MPLCONFIGDIR": os.path.join(cache_root, "mplconfig"),
        "XDG_CACHE_HOME": os.path.join(cache_root, "xdg-cache"),
    }
    for env_var, path in paths.items():
        if env_var not in os.environ:
            os.makedirs(path, exist_ok=True)
            os.environ[env_var] = path


# ── Key generation ────────────────────────────────────────────────────────────
def random_keys(n: int) -> list[bytes]:
    """Generate n shuffled 16-byte keys."""
    keys = [f"{i:016d}".encode() for i in range(n)]
    random.shuffle(keys)
    return keys


# ── RocksDB experiment (python-rocksdb) ──────────────────────────────────────
def run_rocksdb_experiment(write_buf_bytes: int, keys: list[bytes]) -> dict:
    """
    Open a fresh RocksDB with the given write_buffer_size, write all keys,
    and collect timing/latency data.
    """
    import rocksdb

    tmpdir = tempfile.mkdtemp(prefix="exp3_rocksdb_")
    try:
        opts = rocksdb.Options()
        opts.create_if_missing       = True
        opts.write_buffer_size       = write_buf_bytes
        # Allow multiple write buffers so we can observe stall rather than OOM
        opts.max_write_buffer_number = 4
        opts.min_write_buffer_number_to_merge = 2
        # Limit background compaction so stall effects are more visible
        opts.max_background_jobs     = 2
        opts.level0_file_num_compaction_trigger = 4
        opts.level0_slowdown_writes_trigger     = 8
        opts.level0_stop_writes_trigger         = 12

        db = rocksdb.DB(os.path.join(tmpdir, "testdb"), opts)

        batch_latencies = []   # seconds per batch
        stall_count     = 0
        total_start     = time.perf_counter()

        for batch_start in range(0, len(keys), BATCH_SIZE):
            batch_keys = keys[batch_start : batch_start + BATCH_SIZE]
            wb = rocksdb.WriteBatch()
            for k in batch_keys:
                wb.put(k, VALUE_BYTES)

            t0 = time.perf_counter()
            db.write(wb)
            elapsed = time.perf_counter() - t0

            batch_latencies.append(elapsed)

        total_elapsed = time.perf_counter() - total_start

        # ── Detect write stall batches ────────────────────────────────────────
        # A stall shows up as a latency spike compared to the median batch time.
        if batch_latencies:
            import statistics
            median_lat = statistics.median(batch_latencies)
            stall_count = sum(
                1 for lat in batch_latencies
                if lat > SPIKE_THRESHOLD * median_lat
            )

        total_bytes = len(keys) * (KEY_SIZE + VALUE_SIZE)
        throughput_ops  = len(keys) / total_elapsed
        throughput_mbs  = (total_bytes / (1024 * 1024)) / total_elapsed
        est_flush_count = max(1, total_bytes // write_buf_bytes)

        del db
        return {
            "write_buf_mb":       write_buf_bytes // (1024 * 1024),
            "total_elapsed_s":    round(total_elapsed, 4),
            "throughput_ops_s":   round(throughput_ops, 1),
            "throughput_mb_s":    round(throughput_mbs, 3),
            "stall_batch_count":  stall_count,
            "est_flush_count":    est_flush_count,
            "total_batches":      len(batch_latencies),
            "p50_batch_ms":       round(1000 * sorted(batch_latencies)[len(batch_latencies)//2], 4),
            "p99_batch_ms":       round(1000 * sorted(batch_latencies)[int(len(batch_latencies)*0.99)], 4),
        }

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ── Simulation fallback (no python-rocksdb) ──────────────────────────────────
def run_simulation(write_buf_bytes: int, keys: list[bytes]) -> dict:
    """
    Model the WriteBufferManager 90% flush threshold from write_buffer_manager.cc
    without an actual RocksDB instance.  Used when python-rocksdb is unavailable.
    Simulates the memory_active_ counter and counts ShouldFlush() triggers.
    """
    # Approximate bytes per write (key + value + RocksDB internal overhead ≈ 40B)
    ROCKSDB_OVERHEAD = 40
    bytes_per_write = KEY_SIZE + VALUE_SIZE + ROCKSDB_OVERHEAD

    # From write_buffer_manager.cc:
    #   mutable_limit_  = buffer_size_ * 7 / 8   (87.5%)
    #   ShouldFlush() when memory_active_ >= 90% of buffer_size_
    flush_threshold = int(write_buf_bytes * 0.90)
    mutable_limit   = int(write_buf_bytes * 7 / 8)

    memory_active  = 0
    flush_count    = 0
    stall_count    = 0   # would stall if allow_stall=true

    for _ in range(len(keys)):
        memory_active += bytes_per_write
        if memory_active >= write_buf_bytes:
            # Would trigger BeginWriteStall — count as stall
            stall_count += 1
        if memory_active >= flush_threshold:
            # ShouldFlush() → triggers background flush → ScheduleFreeMem
            flush_count  += 1
            # After flush completes, FreeMem() resets active memory
            memory_active = max(0, memory_active - write_buf_bytes)

    total_bytes = len(keys) * bytes_per_write
    # Simulate timing: each write is ~1 μs baseline; stalls add 2 ms each
    base_time  = len(keys) * 1e-6
    stall_time = stall_count * 0.002
    total_elapsed = base_time + stall_time
    throughput_ops = len(keys) / total_elapsed if total_elapsed > 0 else 0
    throughput_mbs = (total_bytes / (1024 * 1024)) / total_elapsed if total_elapsed > 0 else 0

    return {
        "write_buf_mb":       write_buf_bytes // (1024 * 1024),
        "total_elapsed_s":    round(total_elapsed, 4),
        "throughput_ops_s":   round(throughput_ops, 1),
        "throughput_mb_s":    round(throughput_mbs, 3),
        "stall_batch_count":  stall_count,
        "est_flush_count":    flush_count,
        "total_batches":      len(keys) // BATCH_SIZE,
        "p50_batch_ms":       round(base_time * 1000 / (len(keys) // BATCH_SIZE or 1), 4),
        "p99_batch_ms":       round((base_time + stall_time) * 1000 / max(1, stall_count), 4),
        "mode":               "simulation",
    }


# ── Plotting ──────────────────────────────────────────────────────────────────
def plot(rows: list[dict], plots_dir: str, mode: str):
    prepare_plot_env()
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("  matplotlib/numpy not installed — skipping plot.")
        return

    buf_labels    = [f"{r['write_buf_mb']}MB"  for r in rows]
    throughputs   = [r["throughput_mb_s"]       for r in rows]
    stall_counts  = [r["stall_batch_count"]     for r in rows]
    flush_counts  = [r["est_flush_count"]       for r in rows]
    p99_latencies = [r["p99_batch_ms"]          for r in rows]

    x = np.arange(len(rows))
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    mode_note = "  (simulation mode — python-rocksdb unavailable)" if mode == "simulation" else ""
    fig.suptitle(
        f"Exp 3: WriteBufferManager  write_buffer_size  Sweep{mode_note}\n"
        f"write_buffer_manager.cc · alloc_tracker.cc  |  N={NUM_WRITES:,} writes",
        fontsize=11, fontweight="bold"
    )

    colour = "#1565C0"

    # Throughput
    ax = axes[0][0]
    bars = ax.bar(x, throughputs, color=colour, edgecolor="black", linewidth=0.7)
    ax.set_xticks(x); ax.set_xticklabels(buf_labels)
    ax.set_ylabel("Throughput (MB/s)"); ax.set_title("Write Throughput")
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=9); ax.grid(axis="y", alpha=0.3)

    # Estimated flush count
    ax = axes[0][1]
    bars = ax.bar(x, flush_counts, color="#2E7D32", edgecolor="black", linewidth=0.7)
    ax.set_xticks(x); ax.set_xticklabels(buf_labels)
    ax.set_ylabel("Estimated Flush Count"); ax.set_title("Flush Frequency")
    ax.bar_label(bars, fmt="%d", padding=3, fontsize=9); ax.grid(axis="y", alpha=0.3)

    # Stall events
    ax = axes[1][0]
    bars = ax.bar(x, stall_counts, color="#E65100", edgecolor="black", linewidth=0.7)
    ax.set_xticks(x); ax.set_xticklabels(buf_labels)
    ax.set_ylabel("Stall / Spike Events"); ax.set_title("Write Stall Events")
    ax.bar_label(bars, fmt="%d", padding=3, fontsize=9); ax.grid(axis="y", alpha=0.3)

    # p99 batch latency
    ax = axes[1][1]
    ax.plot(x, p99_latencies, color="#7B1FA2", marker="o", linewidth=2, markersize=7)
    ax.set_xticks(x); ax.set_xticklabels(buf_labels)
    ax.set_ylabel("p99 Batch Latency (ms)"); ax.set_title("p99 Write Batch Latency")
    ax.grid(alpha=0.3)

    # Shade the 90% threshold annotation on stall chart
    axes[1][0].set_xlabel("write_buffer_size")
    axes[0][0].set_xlabel("write_buffer_size")
    axes[0][1].set_xlabel("write_buffer_size")
    axes[1][1].set_xlabel("write_buffer_size")

    plt.tight_layout()
    out = os.path.join(plots_dir, "exp3_write_stall.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Plot → {out}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Experiment 3: WriteBufferManager Write-Stall Behavior")
    print("=" * 60)

    # Detect whether python-rocksdb is available
    use_rocksdb = False
    try:
        import rocksdb  # noqa: F401
        use_rocksdb = True
        print("  Mode: python-rocksdb (real RocksDB instance)")
    except ImportError:
        print("  WARNING: python-rocksdb not found.")
        print("    Install with: pip install python-rocksdb")
        print("  Falling back to WriteBufferManager simulation mode.")
        print("  (Models the 90% ShouldFlush() threshold from write_buffer_manager.cc)")

    print(f"  Writes     : {NUM_WRITES:,}")
    print(f"  Value size : {VALUE_SIZE} bytes")
    print(f"  Buffers    : {[str(b // (1024*1024)) + 'MB' for b in BUFFER_SIZES]}")
    print()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR,   exist_ok=True)

    # Pre-generate keys once — same key set for all buffer sizes (controlled)
    print("  Generating keys... ", end="", flush=True)
    keys = random_keys(NUM_WRITES)
    print("done")
    print()

    run_fn = run_rocksdb_experiment if use_rocksdb else run_simulation
    mode   = "rocksdb" if use_rocksdb else "simulation"

    print(f"  {'BufSize':>8}  {'Throughput':>14}  {'Flushes':>10}  {'Stalls':>8}  {'p99 (ms)':>10}")
    print(f"  {'-'*8}  {'-'*14}  {'-'*10}  {'-'*8}  {'-'*10}")

    rows = []
    for buf_size in BUFFER_SIZES:
        buf_label = f"{buf_size // (1024*1024)}MB"
        print(f"  {buf_label:>8}  ", end="", flush=True)
        try:
            result = run_fn(buf_size, keys)
            result["mode"] = mode
            rows.append(result)
            print(
                f"{result['throughput_mb_s']:>12.2f} MB/s  "
                f"{result['est_flush_count']:>10d}  "
                f"{result['stall_batch_count']:>8d}  "
                f"{result['p99_batch_ms']:>10.3f}"
            )
        except Exception as e:
            print(f"ERROR: {e}")

    if not rows:
        print("\nNo results — check setup.")
        sys.exit(1)

    # ── Save CSV ──────────────────────────────────────────────────────────────
    csv_path = os.path.join(RESULTS_DIR, "results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  CSV    → {csv_path}")

    # ── Plot ─────────────────────────────────────────────────────────────────
    plot(rows, PLOTS_DIR, mode)

    # ── Source-code-backed insight ────────────────────────────────────────────
    print()
    print("  Source insights (write_buffer_manager.cc):")
    print("    mutable_limit_  = buffer_size_ * 7 / 8  (87.5% threshold)")
    print("    ShouldFlush()   triggers at 90% memory_active_")
    print("    ShouldStall()   triggers when memory_used_ >= buffer_size_")
    print("    BeginWriteStall() appends writer to queue_ (mutex-guarded)")
    print("    MaybeEndWriteStall() signals all queued writers after FreeMem()")

    smallest = rows[0]
    largest  = rows[-1]
    if largest["throughput_mb_s"] and smallest["throughput_mb_s"]:
        ratio = largest["throughput_mb_s"] / smallest["throughput_mb_s"]
        print(f"\n    Throughput ratio (largest/smallest buffer): {ratio:.2f}x")
    print()


if __name__ == "__main__":
    main()
