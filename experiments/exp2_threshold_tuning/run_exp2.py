#!/usr/bin/env python3
"""
Experiment 2: HashLinkList threshold_use_skiplist Tuning
DS614 Final Project — RocksDB MemTable Layer

Measures how the per-bucket data structure transition threshold in
HashLinkListRep affects write throughput under a random workload.

Source code reference (hash_linklist_rep.cc):
  Each hash bucket starts as an empty slot (Case 1), promotes to a single
  Node (Case 2), then a BucketHeader-headed linked list (Case 3), and finally
  upgrades to a full SkipList bucket (Case 4) when entry count hits threshold:

      if (header->GetNumEntries() == threshold_use_skiplist_) {
          // ... allocate SkipListBucketHeader, move all entries
          bucket.store(new_skip_list_header, std::memory_order_release);
      }

  Low threshold  → buckets upgrade to SkipList early → many small skip lists
                   (expensive per-upgrade, but O(log k) search within bucket)
  High threshold → buckets stay as linked lists longer → O(k) linear search
                   per bucket until threshold hit; cheaper promotion cost

  Sweet spot: threshold should be set so that the expected number of entries
  per bucket (N / bucket_count) approaches the threshold gradually.

Hypothesis:
  There is an optimal threshold range around N/buckets where:
    - Below it: premature promotion wastes allocation overhead
    - Above it: linked-list linear scan dominates bucket lookup time
  With bucket_count=1,000,000 and N=500,000, most buckets hold ~0.5 entries
  on average, so low thresholds (4–16) should perform similarly because most
  buckets never reach the threshold. Very high thresholds degrade for skewed
  key distributions where hot buckets accumulate many entries.

How to run:
  export MEMTABLEREP_BENCH=~/rocksdb/memtablerep_bench
  python3 run_exp2.py
"""

import os
import re
import csv
import sys
import subprocess
import tempfile

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "../../results/exp2"))
PLOTS_DIR   = os.path.normpath(os.path.join(SCRIPT_DIR, "../../results/plots"))

# ── Config ────────────────────────────────────────────────────────────────────
NUM_OPS      = 500_000
ITEM_SIZE    = 100
BUCKET_COUNT = 1_000_000
PREFIX_LEN   = 8
WRITE_BUF_MB = 256

# Threshold values to sweep — spans the interesting range from the source code
# minimum (forced to 3 in hash_linklist_rep.cc constructor) through defaults
THRESHOLDS = [4, 8, 16, 32, 64, 128, 256, 512, 1024]

# Run each threshold for both sequential and random to expose interaction
BENCHMARKS = [
    ("fillrandom", "Random"),
    ("fillseq",    "Sequential"),
]


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


# ── Helpers (same pattern as exp1) ───────────────────────────────────────────
def find_bench() -> str | None:
    env = os.environ.get("MEMTABLEREP_BENCH")
    if env and os.path.isfile(env):
        return env
    candidates = [
        os.path.join(SCRIPT_DIR, "../../memtablerep_bench"),
        os.path.expanduser("~/rocksdb/memtablerep_bench"),
        os.path.expanduser("~/rocksdb/build/memtablerep_bench"),
        "/usr/local/bin/memtablerep_bench",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return os.path.abspath(c)
    return None


def parse_output(stdout: str) -> dict:
    result = {}
    patterns = {
        "elapsed_us":        r"Elapsed time:\s+([\d.]+)\s+us",
        "throughput_mib_s":  r"Write throughput:\s+([\d.]+)\s+MiB/s",
        "write_us_op":       r"write us/op:\s+([\d.]+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, stdout)
        if m:
            result[key] = float(m.group(1))
    return result


def run_bench(binary: str, benchmark: str, threshold: int) -> dict | None:
    cmd = [
        binary,
        f"--benchmarks={benchmark}",
        "--memtablerep=hashlinklist",
        f"--num_operations={NUM_OPS}",
        f"--item_size={ITEM_SIZE}",
        f"--bucket_count={BUCKET_COUNT}",
        f"--prefix_length={PREFIX_LEN}",
        f"--write_buffer_size={WRITE_BUF_MB * 1024 * 1024}",
        f"--threshold_use_skiplist={threshold}",
        "--num_threads=1",
        "--if_log_bucket_dist_when_flash=false",   # suppress noisy log
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            print(f"\n    stderr: {proc.stderr[:300]}", file=sys.stderr)
            return None
        return parse_output(proc.stdout)
    except subprocess.TimeoutExpired:
        print(f"\n    TIMEOUT", file=sys.stderr)
        return None
    except FileNotFoundError:
        print(f"\n    Binary not found: {binary}", file=sys.stderr)
        return None


# ── Plotting ──────────────────────────────────────────────────────────────────
def plot(rows: list[dict], plots_dir: str):
    prepare_plot_env()
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("  matplotlib/numpy not installed — skipping plot.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "Exp 2: HashLinkList  threshold_use_skiplist  Sweep\n"
        f"(hash_linklist_rep.cc · N={NUM_OPS:,} · bucket_count={BUCKET_COUNT:,})",
        fontsize=12, fontweight="bold"
    )

    bench_styles = {
        "Random":     {"color": "#E65100", "marker": "o", "ls": "-"},
        "Sequential": {"color": "#1565C0", "marker": "s", "ls": "--"},
    }

    for ax_idx, (metric, ylabel, title) in enumerate([
        ("throughput_mib_s", "Throughput (MiB/s)",       "Write Throughput vs Threshold"),
        ("write_us_op",      "Avg Write Latency (μs/op)", "Write Latency vs Threshold"),
    ]):
        ax = axes[ax_idx]
        for bench_label, style in bench_styles.items():
            bench_rows = [r for r in rows if r["benchmark_label"] == bench_label]
            if not bench_rows:
                continue
            xs = [r["threshold"] for r in bench_rows]
            ys = [r.get(metric, 0) for r in bench_rows]
            ax.plot(xs, ys, label=bench_label,
                    color=style["color"], marker=style["marker"],
                    linestyle=style["ls"], linewidth=1.8, markersize=6)

        ax.set_xscale("log", base=2)
        ax.set_xlabel("threshold_use_skiplist  (log₂ scale)", fontsize=10)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(alpha=0.35)
        ax.set_xticks(THRESHOLDS)
        ax.set_xticklabels([str(t) for t in THRESHOLDS], fontsize=8)

    # Annotate the transition region
    axes[0].axvspan(32, 128, alpha=0.08, color="green",
                    label="Typical optimal range\n(N/buckets ≈ 0.5)")

    plt.tight_layout()
    out = os.path.join(plots_dir, "exp2_threshold_tuning.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Plot → {out}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Experiment 2: HashLinkList Threshold Tuning")
    print("=" * 60)

    binary = find_bench()
    if not binary:
        print("\nERROR: memtablerep_bench binary not found.")
        print("  export MEMTABLEREP_BENCH=~/rocksdb/memtablerep_bench")
        sys.exit(1)

    print(f"  Binary      : {binary}")
    print(f"  Ops         : {NUM_OPS:,}")
    print(f"  BucketCount : {BUCKET_COUNT:,}")
    print(f"  Thresholds  : {THRESHOLDS}")
    print()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR,   exist_ok=True)

    rows = []
    for bench_flag, bench_label in BENCHMARKS:
        print(f"  Workload: {bench_label}")
        print(f"  {'Threshold':>12}  {'Throughput (MiB/s)':>20}  {'Latency (μs/op)':>18}")
        print(f"  {'-'*12}  {'-'*20}  {'-'*18}")

        for threshold in THRESHOLDS:
            print(f"  {threshold:>12}  ", end="", flush=True)
            metrics = run_bench(binary, bench_flag, threshold)
            if metrics:
                tp  = metrics.get("throughput_mib_s", 0)
                lat = metrics.get("write_us_op", 0)
                rows.append({
                    "benchmark_label": bench_label,
                    "benchmark_flag":  bench_flag,
                    "threshold":       threshold,
                    "throughput_mib_s": tp,
                    "write_us_op":     lat,
                    "elapsed_us":      metrics.get("elapsed_us", ""),
                })
                print(f"{tp:>20.2f}  {lat:>18.4f}")
            else:
                print(f"{'FAILED':>20}  {'FAILED':>18}")
        print()

    if not rows:
        print("No results — check binary.")
        sys.exit(1)

    # ── Save CSV ──────────────────────────────────────────────────────────────
    csv_path = os.path.join(RESULTS_DIR, "results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV    → {csv_path}")

    # ── Plot ─────────────────────────────────────────────────────────────────
    plot(rows, PLOTS_DIR)

    # ── Insight ───────────────────────────────────────────────────────────────
    rand_rows = [r for r in rows if r["benchmark_label"] == "Random"]
    if rand_rows:
        best = max(rand_rows, key=lambda r: r.get("throughput_mib_s", 0))
        worst = min(rand_rows, key=lambda r: r.get("throughput_mib_s", 0))
        print(f"\n  Best  threshold={best['threshold']:>5}: {best['throughput_mib_s']:.2f} MiB/s")
        print(f"  Worst threshold={worst['threshold']:>5}: {worst['throughput_mib_s']:.2f} MiB/s")
        if worst["throughput_mib_s"] > 0:
            ratio = best["throughput_mib_s"] / worst["throughput_mib_s"]
            print(f"  Spread: {ratio:.2f}x  (bucket upgrade policy matters)")
        print()
        print("  Source insight:")
        print("    hash_linklist_rep.cc constructor forces minimum threshold=3.")
        print("    Case 3→4 upgrade: allocates SkipListBucketHeader, iterates all")
        print("    linked-list nodes to re-insert into skip list — O(k log k) cost.")
        print("    Threshold should match expected hot-bucket occupancy.")

    print()


if __name__ == "__main__":
    main()
