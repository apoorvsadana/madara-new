#!/usr/bin/env python3
import argparse
import csv
import re
import sys
from typing import Dict, Any, TextIO


BLOCK_ONLY_LINE_RE = re.compile(r"^(?P<block>\d{6,})$")
STARTING_NEW_BLOCK_RE = re.compile(r"Starting new block (?P<block>\d+)")
RPC_APPEND_BATCH_RE = re.compile(r"RPC\s+madara_V0_1_0_appendBatch\s+\d+\s+\d+ bytes - (?P<ms>[\d.]+)ms")
NONCE_VALIDATION_MS_RE = re.compile(r"append_batch nonce validation: queries=\d+ ms=(?P<ms>\d+)")
NONCE_CHUNK_MS_RE = re.compile(r"get_contract_nonce_at_many: processed chunk items=\d+ ms=(?P<ms>\d+)")
STORAGE_SPLIT_RE = re.compile(r"get_contract_storage_many_split: .* total=(?P<total>\d+)")
STORAGE_VALIDATION_MS_RE = re.compile(r"append_batch storage validation: queries=\d+ ms=(?P<ms>\d+)")
STORAGE_CHUNK_MS_RE = re.compile(r"get_storage_at_many: processed chunk items=\d+ ms=(?P<ms>\d+)")
EXECUTION_STATS_RE = re.compile(
    r"Execution stats: executed=(?P<executed>\d+), added=(?P<added>\d+), reverted=(?P<reverted>\d+), rejected=(?P<rejected>\d+), gas=(?P<gas>\d+), declared_classes=(?P<declared>\d+)"
)
BLOCK_STATUS_RE = re.compile(r"Block status: full=(?P<full>\w+), empty=(?P<empty>\w+), force_close=(?P<force_close>\w+)")
EXEC_ADDED_DURATION_RE = re.compile(r"Executed and added .* - (?P<val>[\d.]+)(?P<Unit>µs|ms)")
COMMITMENTS_MS_RE = re.compile(r"write_new_confirmed_inner: computed commitments .* ms=(?P<ms>\d+)")
STATE_ROOT_MS_RE = re.compile(r"write_new_confirmed_inner: computed state root .* ms=(?P<ms>\d+)")
BLOCK_HASH_MS_RE = re.compile(r"write_new_confirmed_inner: computed block hash .* ms=(?P<ms>\d+)")
CLOSED_BLOCK_DURATION_RE = re.compile(r"Closed block #(?P<block>\d+) with (?P<txs>\d+) transactions - (?P<val>[\d.]+)(?P<Unit>µs|ms)")


def micro_to_milli(val: float, unit: str) -> float:
    if unit == "µs":
        return val / 1000.0
    return val


def ensure_block(metrics: Dict[int, Dict[str, Any]], block: int) -> Dict[str, Any]:
    if block not in metrics:
        metrics[block] = {
            "block": block,
            "tx_executed": None,
            "tx_added": None,
            "gas_used": None,
            "block_full": None,
            "block_empty": None,
            "force_close": None,
            "initial_nonce_validation_ms": None,
            "nonce_chunks_ms_sum": 0.0,
            "initial_storage_validation_ms": None,
            "storage_chunks_ms_sum": 0.0,
            "storage_total_queries": None,
            "append_batch_rpc_ms": None,
            "executed_added_duration_ms": None,
            "commitments_ms": None,
            "state_root_ms": None,
            "block_hash_ms": None,
            "close_block_ms": None,
        }
    return metrics[block]


def parse_log(stream: TextIO) -> Dict[int, Dict[str, Any]]:
    metrics: Dict[int, Dict[str, Any]] = {}
    current_block: int = -1

    for raw_line in stream:
        line = raw_line.strip()
        if not line:
            continue

        m = BLOCK_ONLY_LINE_RE.match(line)
        if m:
            current_block = int(m.group("block"))
            ensure_block(metrics, current_block)
            continue

        m = STARTING_NEW_BLOCK_RE.search(line)
        if m:
            current_block = int(m.group("block"))
            ensure_block(metrics, current_block)
            continue

        # If we don't yet have a block, skip until we find one
        if current_block == -1:
            continue

        blk = ensure_block(metrics, current_block)

        m = NONCE_VALIDATION_MS_RE.search(line)
        if m:
            blk["initial_nonce_validation_ms"] = int(m.group("ms"))
            continue

        m = NONCE_CHUNK_MS_RE.search(line)
        if m:
            blk["nonce_chunks_ms_sum"] = float(blk.get("nonce_chunks_ms_sum", 0.0)) + float(m.group("ms"))
            continue

        m = STORAGE_SPLIT_RE.search(line)
        if m:
            blk["storage_total_queries"] = int(m.group("total"))
            continue

        m = STORAGE_VALIDATION_MS_RE.search(line)
        if m:
            blk["initial_storage_validation_ms"] = int(m.group("ms"))
            continue

        m = STORAGE_CHUNK_MS_RE.search(line)
        if m:
            blk["storage_chunks_ms_sum"] = float(blk.get("storage_chunks_ms_sum", 0.0)) + float(m.group("ms"))
            continue

        m = RPC_APPEND_BATCH_RE.search(line)
        if m:
            blk["append_batch_rpc_ms"] = float(m.group("ms"))
            continue

        m = EXECUTION_STATS_RE.search(line)
        if m:
            blk["tx_executed"] = int(m.group("executed"))
            blk["tx_added"] = int(m.group("added"))
            blk["gas_used"] = int(m.group("gas"))
            continue

        m = BLOCK_STATUS_RE.search(line)
        if m:
            blk["block_full"] = m.group("full")
            blk["block_empty"] = m.group("empty")
            blk["force_close"] = m.group("force_close")
            continue

        m = EXEC_ADDED_DURATION_RE.search(line)
        if m:
            blk["executed_added_duration_ms"] = micro_to_milli(float(m.group("val")), m.group("Unit"))
            continue

        m = COMMITMENTS_MS_RE.search(line)
        if m:
            blk["commitments_ms"] = int(m.group("ms"))
            continue

        m = STATE_ROOT_MS_RE.search(line)
        if m:
            blk["state_root_ms"] = int(m.group("ms"))
            continue

        m = BLOCK_HASH_MS_RE.search(line)
        if m:
            blk["block_hash_ms"] = int(m.group("ms"))
            continue

        m = CLOSED_BLOCK_DURATION_RE.search(line)
        if m:
            # Prefer block parsed earlier, but ensure correct target
            target_block = int(m.group("block"))
            unit = m.group("Unit")
            val = float(m.group("val"))
            blk2 = ensure_block(metrics, target_block)
            blk2["close_block_ms"] = micro_to_milli(val, unit)
            # tx count also available, but we already capture from Execution stats
            continue

    return metrics


def write_csv(metrics: Dict[int, Dict[str, Any]], out: TextIO) -> None:
    fieldnames = [
        "block",
        "tx_executed",
        "tx_added",
        "gas_used",
        "block_full",
        "block_empty",
        "force_close",
        "initial_nonce_validation_ms",
        "nonce_chunks_ms_sum",
        "initial_storage_validation_ms",
        "storage_chunks_ms_sum",
        "storage_total_queries",
        "append_batch_rpc_ms",
        "executed_added_duration_ms",
        "commitments_ms",
        "state_root_ms",
        "block_hash_ms",
        "close_block_ms",
    ]

    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for block in sorted(metrics.keys()):
        row = metrics[block]
        # Ensure numeric aggregations are numbers
        for k in ("nonce_chunks_ms_sum", "storage_chunks_ms_sum"):
            v = row.get(k)
            if v is None:
                row[k] = 0.0
        writer.writerow({k: row.get(k) for k in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Madara logs into a per-block CSV of timings.")
    parser.add_argument("logfile", nargs="?", default="-", help="Path to log file, or '-' for stdin")
    parser.add_argument("--out", dest="outfile", default="-", help="Output CSV file path, or '-' for stdout")
    args = parser.parse_args()

    # Input
    if args.logfile == "-":
        instream = sys.stdin
    else:
        instream = open(args.logfile, "r", encoding="utf-8")

    # Parse
    metrics = parse_log(instream)
    if instream is not sys.stdin:
        instream.close()

    # Output
    if args.outfile == "-":
        outstream = sys.stdout
        write_csv(metrics, outstream)
    else:
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            write_csv(metrics, f)


if __name__ == "__main__":
    main()


