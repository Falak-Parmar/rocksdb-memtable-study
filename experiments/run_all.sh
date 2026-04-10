#!/usr/bin/env bash
# =============================================================================
# DS614 Final Project — RocksDB MemTable Study
# run_all.sh: Reproduce all three experiments end-to-end
#
# Usage:
#   # Recommended — provide the compiled memtablerep_bench binary:
#   export MEMTABLEREP_BENCH=~/rocksdb/memtablerep_bench
#   bash experiments/run_all.sh
#
#   # Or pass it as an argument:
#   bash experiments/run_all.sh /path/to/memtablerep_bench
#
# Building memtablerep_bench (one-time setup):
#   git clone https://github.com/facebook/rocksdb ~/rocksdb
#   cd ~/rocksdb
#   make memtablerep_bench -j$(nproc)       # Linux
#   # macOS: USE_RTTI=1 make memtablerep_bench -j$(sysctl -n hw.ncpu)
#
# Experiments:
#   exp1  SkipList vs HashSkipList vs HashLinkList write throughput
#         Source: skiplistrep.cc, hash_skiplist_rep.cc, hash_linklist_rep.cc
#
#   exp2  HashLinkList threshold_use_skiplist sweep
#         Source: hash_linklist_rep.cc (Case 3→4 bucket upgrade logic)
#
#   exp3  WriteBufferManager write-stall behavior
#         Source: write_buffer_manager.cc, alloc_tracker.cc
#         Requires: python-rocksdb  (falls back to simulation if unavailable)
#
# Results:
#   results/exp1/results.csv
#   results/exp2/results.csv
#   results/exp3/results.csv
#   results/plots/exp1_throughput_latency.png
#   results/plots/exp2_threshold_tuning.png
#   results/plots/exp3_write_stall.png
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*"; }
banner()  { echo -e "\n${BOLD}${CYAN}$*${RESET}\n"; }

# ── Optional argument overrides env var ──────────────────────────────────────
if [[ $# -ge 1 ]]; then
    export MEMTABLEREP_BENCH="$1"
fi

# ── Check binary ─────────────────────────────────────────────────────────────
banner "DS614 Final Project — RocksDB MemTable Experiments"

if [[ -n "${MEMTABLEREP_BENCH:-}" && -f "${MEMTABLEREP_BENCH}" ]]; then
    success "memtablerep_bench : ${MEMTABLEREP_BENCH}"
else
    warn "MEMTABLEREP_BENCH not set or binary not found."
    warn "Exp 1 and Exp 2 require this binary.  Build instructions:"
    warn "  git clone https://github.com/facebook/rocksdb ~/rocksdb"
    warn "  cd ~/rocksdb && make memtablerep_bench -j\$(nproc)"
    warn "  export MEMTABLEREP_BENCH=~/rocksdb/memtablerep_bench"
    echo ""
    warn "Continuing — exp3 will still run (uses python-rocksdb or simulation)."
fi

# ── Python environment ────────────────────────────────────────────────────────
info "Checking Python environment..."

PYTHON="${PYTHON:-python3}"
if ! command -v "${PYTHON}" &>/dev/null; then
    error "Python 3 not found.  Install Python 3.9+ and retry."
    exit 1
fi
success "Python : $("${PYTHON}" --version)"

# Install requirements (non-fatal — python-rocksdb can be tricky)
REQ_FILE="${SCRIPT_DIR}/requirements.txt"
if [[ -f "${REQ_FILE}" ]]; then
    info "Installing Python requirements..."
    "${PYTHON}" -m pip install --quiet -r "${REQ_FILE}" \
        || warn "Some packages failed to install — experiments may fall back."
fi

# ── Create results directories ────────────────────────────────────────────────
info "Creating results directories..."
mkdir -p \
    "${REPO_ROOT}/results/exp1" \
    "${REPO_ROOT}/results/exp2" \
    "${REPO_ROOT}/results/exp3" \
    "${REPO_ROOT}/results/plots"
success "Directories ready under results/"

# ── Experiment runner helper ──────────────────────────────────────────────────
run_experiment() {
    local exp_id="$1"
    local exp_dir="$2"
    local script="$3"

    banner "Experiment ${exp_id}"

    if [[ ! -f "${exp_dir}/${script}" ]]; then
        error "Script not found: ${exp_dir}/${script}"
        return 1
    fi

    # Check binary requirement for exp1/exp2
    if [[ "${exp_id}" =~ ^(1|2)$ ]] && \
       [[ -z "${MEMTABLEREP_BENCH:-}" || ! -f "${MEMTABLEREP_BENCH}" ]]; then
        warn "Skipping Exp ${exp_id} — MEMTABLEREP_BENCH not set."
        return 0
    fi

    local start_ts
    start_ts=$(date +%s)

    if "${PYTHON}" "${exp_dir}/${script}"; then
        local end_ts elapsed
        end_ts=$(date +%s)
        elapsed=$(( end_ts - start_ts ))
        success "Exp ${exp_id} completed in ${elapsed}s"
    else
        error "Exp ${exp_id} failed (exit code $?)"
        return 1
    fi
}

# ── Run all experiments ───────────────────────────────────────────────────────
FAILED=()

run_experiment 1 "${SCRIPT_DIR}/exp1_skiplist_vs_hash"  run_exp1.py \
    || FAILED+=("exp1")

run_experiment 2 "${SCRIPT_DIR}/exp2_threshold_tuning"  run_exp2.py \
    || FAILED+=("exp2")

run_experiment 3 "${SCRIPT_DIR}/exp3_write_stall"       run_exp3.py \
    || FAILED+=("exp3")

# ── Summary ───────────────────────────────────────────────────────────────────
banner "Summary"

if [[ -f "${REPO_ROOT}/results/exp1/results.csv" ]]; then
    success "Exp 1 results : results/exp1/results.csv"
fi
if [[ -f "${REPO_ROOT}/results/exp2/results.csv" ]]; then
    success "Exp 2 results : results/exp2/results.csv"
fi
if [[ -f "${REPO_ROOT}/results/exp3/results.csv" ]]; then
    success "Exp 3 results : results/exp3/results.csv"
fi

echo ""
for plot_file in "${REPO_ROOT}/results/plots"/*.png; do
    [[ -f "${plot_file}" ]] && success "Plot : ${plot_file##*/}"
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo ""
    error "Failed experiments: ${FAILED[*]}"
    echo ""
    echo "  Common fixes:"
    echo "    Exp 1/2: set MEMTABLEREP_BENCH=/path/to/binary"
    echo "    Exp 3:   pip install python-rocksdb  (or run without it for simulation)"
    exit 1
fi

echo ""
success "All experiments completed successfully."
echo ""
echo "  To view results:"
echo "    open ${REPO_ROOT}/results/plots/"
echo "    cat  ${REPO_ROOT}/results/exp1/results.csv"
echo ""
