# RocksDB MemTable Study — DS614 Final Project

This project studies RocksDB's memtable layer, the in-memory write buffer used before data is flushed into the LSM tree. The repository combines source-level analysis with reproducible experiments on different memtable representations and write-buffer behavior.

## Team

- 202518035 - Aditya Jana
- 202518053 - Falak Parmar

## Repository layout

- `source/` - RocksDB memtable-related source files used for study
- `experiments/` - scripts for all three experiments and the `run_all.sh` driver
- `results/` - generated CSV outputs and plots
- `report/report.md` - project report
- `slides/presentation.pdf` - presentation slides

## Build requirements

- Python 3.9+
- `pip` packages from `requirements.txt`
- A compiled RocksDB `memtablerep_bench` binary for Experiments 1 and 2

## Quick start

```bash
# 1. Build memtablerep_bench from RocksDB source (one-time)
git clone https://github.com/facebook/rocksdb ~/rocksdb
cd ~/rocksdb
make memtablerep_bench -j$(nproc)

# On macOS, use:
# USE_RTTI=1 make memtablerep_bench -j$(sysctl -n hw.ncpu)

# 2. Set the binary path
export MEMTABLEREP_BENCH=~/rocksdb/memtablerep_bench

# 3. Install Python dependencies
cd /path/to/rocksdb-memtable-study
pip install -r requirements.txt

# 4. Run all experiments
bash experiments/run_all.sh
```

Results are written under `results/`, and plots are generated in `results/plots/`.

## Reproducing experiments

```bash
# Run all experiments
bash experiments/run_all.sh

# Or pass the benchmark binary explicitly
bash experiments/run_all.sh /path/to/memtablerep_bench
```

## Experiment descriptions

### Exp 1 — SkipList vs HashSkipList vs HashLinkList

**Script:** `experiments/exp1_skiplist_vs_hash/run_exp1.py`

Compares three RocksDB MemTable implementations under sequential and random write workloads using `memtablerep_bench`.

| Source file | Implementation |
| --- | --- |
| `skiplistrep.cc` | `SkipListRep` wrapping `InlineSkipList` |
| `hash_skiplist_rep.cc` | `HashSkipListRep` using hash buckets with per-bucket skip lists |
| `hash_linklist_rep.cc` | `HashLinkListRep` using hash buckets with linked-list to skip-list promotion |

**Hypothesis:** `HashSkipListRep` should perform better for random writes because each bucket narrows the search scope, while `SkipListRep` may benefit on sequential writes because of the `seq_splice_` fast path in `InlineSkipList`.

### Exp 2 — HashLinkList `threshold_use_skiplist` sweep

**Script:** `experiments/exp2_threshold_tuning/run_exp2.py`

Varies `threshold_use_skiplist` across:

`[4, 8, 16, 32, 64, 128, 256, 512, 1024]`

The experiment measures how the promotion threshold affects write throughput.

**Source focus:** `hash_linklist_rep.cc`, where a bucket is upgraded to a skip-list-backed structure once `GetNumEntries() == threshold_use_skiplist_`.

### Exp 3 — WriteBufferManager write-stall behavior

**Script:** `experiments/exp3_write_stall/run_exp3.py`

Studies how `write_buffer_size` influences flush frequency, stall behavior, and p99 write latency.

**Source focus:** `write_buffer_manager.cc`

- `ShouldFlush()` starts flush pressure at about 90% of `buffer_size_`
- `BeginWriteStall()` can suspend writers when memory usage reaches the configured limit and stalling is allowed

If `python-rocksdb` is unavailable, the script falls back to a simulation of the threshold logic.

## Running experiments individually

```bash
python3 experiments/exp1_skiplist_vs_hash/run_exp1.py
python3 experiments/exp2_threshold_tuning/run_exp2.py
python3 experiments/exp3_write_stall/run_exp3.py
```

## Outputs

Expected outputs include:

- `results/exp1/results.csv`
- `results/exp2/results.csv`
- `results/exp3/results.csv`
- `results/plots/exp1_throughput_latency.png`
- `results/plots/exp2_threshold_tuning.png`
- `results/plots/exp3_write_stall.png`

## Report and slides

- Report: `report/report.md`
- Slides: `slides/presentation.pdf`
