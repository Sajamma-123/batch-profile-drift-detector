"""100,000+ Row Performance Benchmark Script."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
import pandas as pd
import yaml

from pipeline import execute_pipeline
from src.db import get_connection, init_db, seed_reference_customers
from src.generator import generate_transactions
from src.reconciler import run_reconciliation


def run_benchmark(n_rows: int = 100_000, seed: int = 42) -> None:
    print("=" * 70)
    print(f"STARTING PERFORMANCE BENCHMARK: {n_rows:,} ROWS")
    print("=" * 70)

    bench_dir = Path("data/benchmark")
    bench_dir.mkdir(parents=True, exist_ok=True)
    bench_csv = bench_dir / f"benchmark_{n_rows}.csv"
    bench_db = bench_dir / "bench_pipeline.db"
    bench_cfg_path = bench_dir / "bench_config.yaml"

    # Config with isolated DB
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(bench_db)
    with open(bench_cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f)

    # Clean existing benchmark db if present
    if bench_db.exists():
        try:
            os.remove(bench_db)
        except Exception:
            pass

    init_db(bench_db)
    seed_reference_customers(bench_db, count=500)

    # 1. Benchmark Data Generation
    t0 = time.perf_counter()
    print(f"\n[1/4] Generating {n_rows:,} synthetic records (seed={seed})...")
    generate_transactions(
        n_rows=n_rows,
        seed=seed,
        scenario="normal",
        output_path=bench_csv,
    )
    t_gen = time.perf_counter() - t0
    file_size_mb = os.path.getsize(bench_csv) / (1024 * 1024)
    print(f"      Generation Time: {t_gen:.2f}s ({n_rows / t_gen:,.0f} rows/sec, file size: {file_size_mb:.2f} MB)")

    # 2. Benchmark Pipeline Execution
    print(f"\n[2/4] Executing Pipeline (Profiling + Validation + Ingestion)...")
    t1 = time.perf_counter()
    res = execute_pipeline(
        input_path=bench_csv,
        run_id=f"bench_run_{n_rows}",
        config_path=bench_cfg_path,
        set_baseline=True,
        baseline_id="bench_baseline",
        quiet=False,
    )
    t_pipeline = time.perf_counter() - t1
    throughput = n_rows / t_pipeline if t_pipeline > 0 else 0

    print(f"\n[3/4] Execution Performance Summary:")
    print(f"      Total Pipeline Duration: {t_pipeline:.2f} seconds")
    print(f"      Pipeline Throughput:    {throughput:,.0f} rows/sec")
    print(f"      Input Rows:              {res['input_count']:,}")
    print(f"      Valid Rows Ingested:     {res['valid_count']:,}")
    print(f"      Quarantined Rows:        {res['quarantine_count']:,}")

    # 3. Reconciliation verification
    print(f"\n[4/4] Verifying 100% Data Reconciliation...")
    conn = get_connection(bench_db)
    recon = run_reconciliation(conn, run_id=f"bench_run_{n_rows}")
    conn.close()

    assert recon["all_passed"] is True
    print(f"      Reconciliation Check: PASSED (Discrepancy: {recon['runs'][0]['diff']} rows)")

    print("\n" + "=" * 70)
    print("BENCHMARK COMPLETED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    run_benchmark(n_rows=n)
