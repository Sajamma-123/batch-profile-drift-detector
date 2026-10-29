"""Main Pipeline CLI and Execution Orchestrator."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import yaml

from src.alert_engine import Alert, evaluate_alerts, save_alerts
from src.comparator import compare_profiles
from src.db import clean_run_data, get_connection, init_db, seed_reference_customers
from src.generator import generate_transactions
from src.profiler import compute_profile, load_baseline_profile, save_baseline_profile, save_batch_profile
from src.reconciler import format_reconciliation_report, run_reconciliation
from src.validator import validate_batch


def load_config(config_path: str | Path = "config.yaml") -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def execute_pipeline(
    input_path: str | Path,
    run_id: str,
    config_path: str | Path = "config.yaml",
    set_baseline: bool = False,
    baseline_id: str = "default_baseline",
    quiet: bool = False,
) -> Dict[str, Any]:
    """Execute complete data pipeline for an input batch."""
    config = load_config(config_path)
    db_path = config.get("database", {}).get("path", "pipeline.db")

    init_db(db_path)
    seed_reference_customers(db_path)

    start_iso = datetime.now(timezone.utc).isoformat()
    conn = get_connection(db_path)

    # Idempotent cleanup: remove previous run records for this run_id
    clean_run_data(conn, run_id)

    # Initial run_log entry
    with conn:
        conn.execute(
            """
            INSERT INTO run_log 
            (run_id, source_file, start_time, status, input_count, output_count, rejected_count, alerts_count)
            VALUES (?, ?, ?, ?, 0, 0, 0, 0);
            """,
            (run_id, str(input_path), start_iso, "RUNNING"),
        )

    alerts: List[Alert] = []
    failure_reason: Optional[str] = None
    input_count = 0
    valid_count = 0
    quarantine_count = 0

    try:
        in_file = Path(input_path)
        if not in_file.exists():
            raise FileNotFoundError(f"Input file does not exist: {in_file}")

        # Ingest CSV
        # Check if file is completely empty (0 bytes)
        if os.path.getsize(in_file) == 0:
            df = pd.DataFrame()
        else:
            try:
                df = pd.read_csv(in_file, dtype=str)  # Read all as string to preserve raw representations
            except pd.errors.EmptyDataError:
                df = pd.DataFrame()

        input_count = len(df)

        # 1. Compute statistical profile on incoming batch
        key_cols = config.get("profiling", {}).get("key_columns")
        top_n = config.get("profiling", {}).get("top_n_categories", 5)
        batch_profile = compute_profile(df, key_columns=key_cols, top_n=top_n)

        # 2. Baseline or Comparison
        if set_baseline:
            save_baseline_profile(conn, baseline_id, batch_profile)
            if not quiet:
                print(f"[BASELINE] Saved baseline profile '{baseline_id}' ({input_count} rows).")
        else:
            baseline_profile = load_baseline_profile(conn, baseline_id)
            if baseline_profile is not None:
                deltas = compare_profiles(baseline_profile, batch_profile)
                alerts = evaluate_alerts(deltas, config)
                save_alerts(conn, run_id, alerts)
            else:
                if not quiet:
                    print(f"[INFO] No baseline profile found for '{baseline_id}'. Run with --set-baseline to establish one.")

        # 3. Ingestion Validation & Quarantine Routing
        val_result = validate_batch(
            df=df,
            conn=conn,
            config=config,
            run_id=run_id,
            source_file=str(input_path),
        )

        valid_df = val_result.valid_df
        quarantine_records = val_result.quarantine_records
        valid_count = len(valid_df)
        quarantine_count = len(quarantine_records)

        # 4. Insert valid rows into transactions
        with conn:
            if not valid_df.empty:
                # Reorder / keep target columns
                target_cols = [
                    "transaction_id", "customer_id", "amount", "currency",
                    "status", "timestamp", "merchant_id", "run_id", "ingested_at"
                ]
                for c in target_cols:
                    if c not in valid_df.columns:
                        valid_df[c] = None
                
                rows_to_insert = list(valid_df[target_cols].itertuples(index=False, name=None))
                conn.executemany(
                    f"""
                    INSERT INTO transactions ({','.join(target_cols)})
                    VALUES ({','.join(['?'] * len(target_cols))});
                    """,
                    rows_to_insert,
                )

            # Insert quarantine records
            if quarantine_records:
                conn.executemany(
                    """
                    INSERT INTO quarantine 
                    (run_id, source_file, raw_record, quarantine_reason, quarantined_at)
                    VALUES (:run_id, :source_file, :raw_record, :quarantine_reason, :quarantined_at);
                    """,
                    quarantine_records,
                )

        # 5. Save batch profile
        save_batch_profile(conn, run_id, baseline_id, batch_profile)

        # Determine final status
        if not val_result.is_schema_valid:
            status = "REJECTED_SCHEMA"
            failure_reason = "; ".join(val_result.schema_errors)
        elif len(alerts) > 0 and valid_count > 0:
            status = "COMPLETED_WITH_ALERTS"
        elif valid_count == 0 and input_count > 0:
            status = "ALL_QUARANTINED"
        else:
            status = "SUCCESS"

    except Exception as e:
        status = "FAILED"
        failure_reason = str(e)
        if not quiet:
            print(f"[ERROR] Pipeline run failed: {e}", file=sys.stderr)
        raise
    finally:
        end_iso = datetime.now(timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                UPDATE run_log 
                SET end_time = ?, status = ?, input_count = ?, output_count = ?, 
                    rejected_count = ?, alerts_count = ?, failure_reason = ?
                WHERE run_id = ?;
                """,
                (
                    end_iso,
                    status,
                    input_count,
                    valid_count,
                    quarantine_count,
                    len(alerts),
                    failure_reason,
                    run_id,
                ),
            )
        conn.close()

    if not quiet:
        print_run_summary(
            run_id=run_id,
            source_file=str(input_path),
            status=status,
            input_count=input_count,
            valid_count=valid_count,
            quarantine_count=quarantine_count,
            alerts=alerts,
        )

    return {
        "run_id": run_id,
        "status": status,
        "input_count": input_count,
        "valid_count": valid_count,
        "quarantine_count": quarantine_count,
        "alerts": [a.to_dict() for a in alerts],
    }


def print_run_summary(
    run_id: str,
    source_file: str,
    status: str,
    input_count: int,
    valid_count: int,
    quarantine_count: int,
    alerts: List[Alert],
) -> None:
    """Print readable summary to stdout."""
    print("\n" + "=" * 60)
    print(f"PIPELINE RUN SUMMARY: {run_id} ({status})")
    print("=" * 60)
    print(f"Source file:      {source_file}")
    print(f"Total Rows In:    {input_count}")
    print(f"Valid Rows Loaded:{valid_count}")
    print(f"Quarantined Rows: {quarantine_count}")
    print(f"Alerts Triggered: {len(alerts)}")

    if alerts:
        print("\n--- EXPLAINABLE ALERTS ---")
        for a in alerts:
            print(a.format_alert())
            print()
    print("=" * 60 + "\n")


def generate_sample_suite(data_dir: str = "data") -> None:
    """Generate reference datasets for testing and demonstration."""
    out_dir = Path(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating sample data suite in directory:", out_dir)
    # 1. Baseline dataset (100 rows, clean)
    p_base = out_dir / "batch_001_baseline.csv"
    generate_transactions(n_rows=100, seed=42, scenario="baseline", output_path=p_base)
    print(f"  [+] Created {p_base} (100 rows, baseline)")

    # 2. Normal subsequent batch (100 rows, normal)
    p_norm = out_dir / "batch_002_normal.csv"
    generate_transactions(n_rows=100, seed=43, scenario="normal", output_path=p_norm)
    print(f"  [+] Created {p_norm} (100 rows, normal)")

    # 3. Drifted nulls batch (null_rate customer_id ~ 18%)
    p_nulls = out_dir / "batch_003_drift_nulls.csv"
    generate_transactions(n_rows=100, seed=44, scenario="drift_nulls", output_path=p_nulls)
    print(f"  [+] Created {p_nulls} (100 rows, drifted nulls)")

    # 4. Drifted amount batch (mean amount shifted ~ 3x)
    p_amt = out_dir / "batch_004_drift_amount.csv"
    generate_transactions(n_rows=100, seed=45, scenario="drift_amount", output_path=p_amt)
    print(f"  [+] Created {p_amt} (100 rows, drifted amount)")

    # 5. Outdated / Stale timestamps batch
    p_fresh = out_dir / "batch_005_drift_freshness.csv"
    generate_transactions(n_rows=100, seed=46, scenario="drift_freshness", output_path=p_fresh)
    print(f"  [+] Created {p_fresh} (100 rows, stale timestamp)")

    # 6. Corrupted batch (malformed values, duplicate keys, missing fields, bad FK)
    p_corr = out_dir / "batch_006_corrupted.csv"
    generate_transactions(n_rows=100, seed=47, scenario="corrupted", output_path=p_corr)
    print(f"  [+] Created {p_corr} (100 rows, corrupted edge cases)")

    # 7. Empty batch
    p_empty = out_dir / "batch_007_empty.csv"
    pd.DataFrame(columns=[
        "transaction_id", "customer_id", "amount", "currency", "status", "timestamp", "merchant_id"
    ]).to_csv(p_empty, index=False)
    print(f"  [+] Created {p_empty} (0 rows, empty)")

    print("Sample suite generation complete!\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Data Pipeline with Profiling, Drift Alerts, and Quarantine.")
    parser.add_argument("--input", type=str, help="Path to input batch CSV file.")
    parser.add_argument("--run-id", type=str, help="Unique identifier for this pipeline run.")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to configuration YAML file.")
    parser.add_argument("--set-baseline", action="store_true", help="Set this batch as the baseline profile.")
    parser.add_argument("--baseline-id", type=str, default="default_baseline", help="Baseline profile ID identifier.")
    parser.add_argument("--reconcile", action="store_true", help="Run reconciliation checks and output report.")
    parser.add_argument("--generate-samples", action="store_true", help="Generate demonstration sample datasets.")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-error summary console output.")

    args = parser.parse_args()

    if args.generate_samples:
        generate_sample_suite()
        return

    config = load_config(args.config)
    db_path = config.get("database", {}).get("path", "pipeline.db")

    if args.reconcile:
        init_db(db_path)
        conn = get_connection(db_path)
        recon_data = run_reconciliation(conn, run_id=args.run_id)
        conn.close()
        print(format_reconciliation_report(recon_data))
        return

    if not args.input or not args.run_id:
        parser.print_help()
        sys.exit(1)

    execute_pipeline(
        input_path=args.input,
        run_id=args.run_id,
        config_path=args.config,
        set_baseline=args.set_baseline,
        baseline_id=args.baseline_id,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()
