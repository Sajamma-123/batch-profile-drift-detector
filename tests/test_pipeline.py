"""Comprehensive test suite for data pipeline, validation, profiling, alerts, and idempotency."""

import os
import sqlite3
from pathlib import Path
import pandas as pd
import pytest
import yaml

from pipeline import execute_pipeline
from src.alert_engine import Alert, evaluate_alerts
from src.comparator import compare_profiles
from src.db import clean_run_data, get_connection, init_db, seed_reference_customers
from src.generator import generate_transactions
from src.profiler import compute_profile, load_baseline_profile, save_baseline_profile
from src.reconciler import run_reconciliation
from src.validator import validate_batch


@pytest.fixture
def test_env(tmp_path):
    """Set up an isolated test database and configuration."""
    db_file = tmp_path / "test_pipeline.db"
    config_file = tmp_path / "test_config.yaml"

    config_data = {
        "database": {"path": str(db_file)},
        "schema": {
            "primary_key": "transaction_id",
            "reference_table": "customers",
            "reference_key": "customer_id",
            "required_fields": [
                "transaction_id",
                "customer_id",
                "amount",
                "currency",
                "status",
                "timestamp",
            ],
            "allowed_currencies": ["USD", "EUR", "GBP", "CAD", "JPY"],
            "allowed_statuses": ["COMPLETED", "PENDING", "FAILED", "CANCELLED"],
        },
        "thresholds": {
            "null_rate_delta": 0.10,
            "row_count_drop_pct": 0.25,
            "row_count_surge_pct": 1.00,
            "mean_shift_pct": 0.20,
            "freshness_lag_days": 3.0,
            "distinct_count_drop_pct": 0.30,
        },
        "profiling": {
            "key_columns": [
                "transaction_id",
                "customer_id",
                "amount",
                "currency",
                "status",
                "timestamp",
            ],
            "top_n_categories": 5,
        },
    }

    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(config_data, f)

    init_db(db_file)
    seed_reference_customers(db_file, count=500)

    return {
        "db_path": db_file,
        "config_path": config_file,
        "config": config_data,
        "tmp_path": tmp_path,
    }


def test_happy_path(test_env):
    """Test 1: Happy path - clean batch ingests with zero quarantine and no alerts."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    base_csv = tmp / "base.csv"
    batch_csv = tmp / "clean_batch.csv"

    # Generate clean baseline and clean batch
    generate_transactions(n_rows=50, seed=10, scenario="baseline", output_path=base_csv)
    generate_transactions(n_rows=50, seed=11, scenario="normal", output_path=batch_csv)

    # Establish baseline
    res_base = execute_pipeline(base_csv, run_id="run_base", config_path=cfg, set_baseline=True)
    assert res_base["status"] in ("SUCCESS", "COMPLETED_WITH_ALERTS")

    # Ingest clean batch
    res_batch = execute_pipeline(batch_csv, run_id="run_happy", config_path=cfg)
    assert res_batch["status"] == "SUCCESS"
    assert res_batch["input_count"] == 50
    assert res_batch["valid_count"] >= 48
    assert len(res_batch["alerts"]) == 0


def test_empty_batch(test_env):
    """Test 2: Empty batch is handled safely without crashing and fires volume alert."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    base_csv = tmp / "base.csv"
    empty_csv = tmp / "empty.csv"

    generate_transactions(n_rows=50, seed=10, scenario="baseline", output_path=base_csv)
    execute_pipeline(base_csv, run_id="run_base", config_path=cfg, set_baseline=True)

    # Create empty CSV with headers
    pd.DataFrame(columns=[
        "transaction_id", "customer_id", "amount", "currency", "status", "timestamp"
    ]).to_csv(empty_csv, index=False)

    res_empty = execute_pipeline(empty_csv, run_id="run_empty", config_path=cfg)
    assert res_empty["input_count"] == 0
    assert res_empty["valid_count"] == 0
    assert res_empty["quarantine_count"] == 0

    # Must alert on row_count drop
    alert_metrics = [a["metric"] for a in res_empty["alerts"]]
    assert "row_count" in alert_metrics


def test_malformed_record(test_env):
    """Test 3: Non-numeric amount or malformed timestamp is quarantined with exact reason."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file = tmp / "malformed.csv"

    data = [
        {"transaction_id": "TX_M1", "customer_id": "CUST_0001", "amount": "NOT_A_NUMBER", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_M2", "customer_id": "CUST_0002", "amount": "99.50", "currency": "USD", "status": "COMPLETED", "timestamp": "INVALID_TIMESTAMP_STRING"},
        {"transaction_id": "TX_M3", "customer_id": "CUST_0003", "amount": "50.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
    ]
    pd.DataFrame(data).to_csv(csv_file, index=False)

    res = execute_pipeline(csv_file, run_id="run_malformed", config_path=cfg)
    assert res["input_count"] == 3
    assert res["valid_count"] == 1
    assert res["quarantine_count"] == 2

    conn = get_connection(test_env["db_path"])
    cursor = conn.cursor()
    cursor.execute("SELECT quarantine_reason FROM quarantine WHERE run_id = 'run_malformed';")
    reasons = [row[0] for row in cursor.fetchall()]
    conn.close()

    assert any("MALFORMED_RECORD: 'amount'" in r for r in reasons)
    assert any("MALFORMED_RECORD: 'timestamp'" in r for r in reasons)


def test_duplicate_key_intra_batch_and_database(test_env):
    """Test 4: Intra-batch and database cross-batch duplicates are quarantined."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file_1 = tmp / "dup_batch_1.csv"
    csv_file_2 = tmp / "dup_batch_2.csv"

    # Batch 1 with intra-batch duplicate
    data1 = [
        {"transaction_id": "TX_DUP1", "customer_id": "CUST_0001", "amount": "10.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_DUP1", "customer_id": "CUST_0002", "amount": "20.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:01:00Z"},
        {"transaction_id": "TX_UNIQUE", "customer_id": "CUST_0003", "amount": "30.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:02:00Z"},
    ]
    pd.DataFrame(data1).to_csv(csv_file_1, index=False)
    res1 = execute_pipeline(csv_file_1, run_id="run_dup_1", config_path=cfg)

    assert res1["valid_count"] == 2
    assert res1["quarantine_count"] == 1

    # Batch 2 attempts to insert TX_UNIQUE again (database duplicate)
    data2 = [
        {"transaction_id": "TX_UNIQUE", "customer_id": "CUST_0004", "amount": "40.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T11:00:00Z"},
    ]
    pd.DataFrame(data2).to_csv(csv_file_2, index=False)
    res2 = execute_pipeline(csv_file_2, run_id="run_dup_2", config_path=cfg)

    assert res2["valid_count"] == 0
    assert res2["quarantine_count"] == 1

    conn = get_connection(test_env["db_path"])
    cursor = conn.cursor()
    cursor.execute("SELECT quarantine_reason FROM quarantine WHERE run_id = 'run_dup_2';")
    reason = cursor.fetchone()[0]
    conn.close()
    assert "DUPLICATE_KEY_DATABASE" in reason


def test_nulls_in_required_field(test_env):
    """Test 5: Null values in required fields are rejected and quarantined."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file = tmp / "nulls.csv"

    data = [
        {"transaction_id": "TX_N1", "customer_id": None, "amount": "10.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_N2", "customer_id": "CUST_0001", "amount": None, "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_N3", "customer_id": "CUST_0002", "amount": "15.00", "currency": None, "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_N4", "customer_id": "CUST_0003", "amount": "20.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
    ]
    pd.DataFrame(data).to_csv(csv_file, index=False)

    res = execute_pipeline(csv_file, run_id="run_nulls", config_path=cfg)
    assert res["valid_count"] == 1
    assert res["quarantine_count"] == 3


def test_edge_case_invalid_reference_key(test_env):
    """Test 6 (Domain Edge Case 1): Non-existent customer_id rejected by foreign key validation."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file = tmp / "bad_fk.csv"

    data = [
        {"transaction_id": "TX_FK1", "customer_id": "CUST_NONEXISTENT_9999", "amount": "100.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_FK2", "customer_id": "CUST_0001", "amount": "100.00", "currency": "USD", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
    ]
    pd.DataFrame(data).to_csv(csv_file, index=False)

    res = execute_pipeline(csv_file, run_id="run_bad_fk", config_path=cfg)
    assert res["valid_count"] == 1
    assert res["quarantine_count"] == 1

    conn = get_connection(test_env["db_path"])
    cursor = conn.cursor()
    cursor.execute("SELECT quarantine_reason FROM quarantine WHERE run_id = 'run_bad_fk';")
    reason = cursor.fetchone()[0]
    conn.close()
    assert "INVALID_REFERENCE_KEY" in reason


def test_edge_case_late_out_of_order_data(test_env):
    """Test 7 (Domain Edge Case 2): Late/stale batch fires freshness lag alert."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    base_csv = tmp / "base.csv"
    stale_csv = tmp / "stale.csv"

    # Base batch at 2026-10-20
    generate_transactions(n_rows=50, seed=10, scenario="baseline", output_path=base_csv)
    execute_pipeline(base_csv, run_id="run_base", config_path=cfg, set_baseline=True)

    # Stale batch with timestamps from 30 days prior
    generate_transactions(n_rows=50, seed=11, scenario="drift_freshness", output_path=stale_csv)
    res_stale = execute_pipeline(stale_csv, run_id="run_stale", config_path=cfg)

    freshness_alerts = [a for a in res_stale["alerts"] if a["metric"] == "freshness_lag"]
    assert len(freshness_alerts) >= 1
    assert freshness_alerts[0]["severity"] == "HIGH"
    assert "stale by" in freshness_alerts[0]["message"]


def test_edge_case_schema_change_missing_column(test_env):
    """Test 8 (Domain Edge Case 3): Missing required column triggers SCHEMA_MISMATCH quarantine."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    bad_schema_csv = tmp / "bad_schema.csv"

    # Missing 'amount' and 'currency' columns
    data = [
        {"transaction_id": "TX_S1", "customer_id": "CUST_0001", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
        {"transaction_id": "TX_S2", "customer_id": "CUST_0002", "status": "COMPLETED", "timestamp": "2026-10-25T10:00:00Z"},
    ]
    pd.DataFrame(data).to_csv(bad_schema_csv, index=False)

    res = execute_pipeline(bad_schema_csv, run_id="run_schema_err", config_path=cfg)
    assert res["status"] == "REJECTED_SCHEMA"
    assert res["valid_count"] == 0
    assert res["quarantine_count"] == 2

    conn = get_connection(test_env["db_path"])
    cursor = conn.cursor()
    cursor.execute("SELECT quarantine_reason FROM quarantine WHERE run_id = 'run_schema_err';")
    reason = cursor.fetchone()[0]
    conn.close()
    assert "SCHEMA_MISMATCH" in reason


def test_idempotent_load(test_env):
    """Test 9: Rerunning identical batch with same run_id does not duplicate database rows."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file = tmp / "idempotent.csv"

    generate_transactions(n_rows=50, seed=123, scenario="normal", output_path=csv_file)

    # First run
    execute_pipeline(csv_file, run_id="run_idem", config_path=cfg)
    conn = get_connection(test_env["db_path"])
    c1 = conn.execute("SELECT COUNT(*) FROM transactions WHERE run_id = 'run_idem';").fetchone()[0]
    conn.close()

    # Rerun the exact same batch
    execute_pipeline(csv_file, run_id="run_idem", config_path=cfg)
    conn = get_connection(test_env["db_path"])
    c2 = conn.execute("SELECT COUNT(*) FROM transactions WHERE run_id = 'run_idem';").fetchone()[0]
    conn.close()

    assert c1 == c2
    assert c1 > 0


def test_reconciliation_integrity(test_env):
    """Test 10: Reconciliation guarantees input == valid + quarantined with 0 diff."""
    tmp = test_env["tmp_path"]
    cfg = test_env["config_path"]
    csv_file = tmp / "recon.csv"

    generate_transactions(n_rows=100, seed=42, scenario="corrupted", output_path=csv_file)
    execute_pipeline(csv_file, run_id="run_recon_test", config_path=cfg)

    conn = get_connection(test_env["db_path"])
    recon = run_reconciliation(conn, run_id="run_recon_test")
    conn.close()

    assert recon["all_passed"] is True
    run_info = recon["runs"][0]
    assert run_info["diff"] == 0
    assert run_info["input_count"] == run_info["target_count"] + run_info["quarantine_count"]


def test_alert_phrasing_format():
    """Test 11: Alert format matches required human-readable specification exactly."""
    alert = Alert(
        metric="null_rate",
        column="customer_id",
        severity="HIGH",
        baseline_value=0.02,
        current_value=0.18,
        delta=0.16,
        threshold=0.10,
        message="Null rate increased 16 points vs baseline",
    )
    formatted = alert.format_alert()
    expected = (
        "ALERT [null_rate] column=customer_id severity=HIGH\n"
        "  baseline=0.02  current=0.18  delta=+0.16  threshold=0.10\n"
        '  message: "Null rate increased 16 points vs baseline"'
    )
    assert formatted.strip() == expected.strip()
