"""Database setup and helper functions for SQLite."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Create and return a SQLite database connection with row factory."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Enable foreign keys and WAL mode for better write performance
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db(db_path: str | Path) -> None:
    """Initialize database tables if they do not already exist."""
    conn = get_connection(db_path)
    with conn:
        # Reference table: customers
        conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """)

        # Main ingested transactions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                status TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                merchant_id TEXT,
                run_id TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            );
        """)

        # Quarantine table for invalid or rejected records
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quarantine (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_file TEXT,
                raw_record TEXT NOT NULL,
                quarantine_reason TEXT NOT NULL,
                quarantined_at TEXT NOT NULL
            );
        """)

        # Baseline profiles table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS baseline_profiles (
                profile_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                min_timestamp TEXT,
                max_timestamp TEXT,
                metrics_json TEXT NOT NULL
            );
        """)

        # Batch profiles table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_profiles (
                run_id TEXT PRIMARY KEY,
                profile_id TEXT,
                created_at TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                min_timestamp TEXT,
                max_timestamp TEXT,
                metrics_json TEXT NOT NULL
            );
        """)

        # Alerts table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                column_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                baseline_value REAL,
                current_value REAL,
                delta REAL,
                threshold REAL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """)

        # Run log and metrics table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                run_id TEXT PRIMARY KEY,
                source_file TEXT,
                start_time TEXT NOT NULL,
                end_time TEXT,
                status TEXT NOT NULL,
                input_count INTEGER NOT NULL DEFAULT 0,
                output_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                alerts_count INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT
            );
        """)

        # Useful indices
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_run ON transactions(run_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_quar_run ON quarantine(run_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_run ON alerts(run_id);")
    conn.close()


def seed_reference_customers(db_path: str | Path, count: int = 500) -> None:
    """Populate reference customers table if empty."""
    conn = get_connection(db_path)
    with conn:
        existing = conn.execute("SELECT COUNT(*) as cnt FROM customers;").fetchone()["cnt"]
        if existing == 0:
            customers = [
                (
                    f"CUST_{i:04d}",
                    f"Customer {i}",
                    "2026-01-01T00:00:00Z"
                )
                for i in range(1, count + 1)
            ]
            conn.executemany(
                "INSERT INTO customers (customer_id, name, created_at) VALUES (?, ?, ?);",
                customers
            )
    conn.close()


def clean_run_data(conn: sqlite3.Connection, run_id: str) -> None:
    """
    Ensure idempotency: remove previous results for this run_id so a rerun
    replaces data cleanly without duplicates or stale logs.
    """
    conn.execute("DELETE FROM transactions WHERE run_id = ?;", (run_id,))
    conn.execute("DELETE FROM quarantine WHERE run_id = ?;", (run_id,))
    conn.execute("DELETE FROM batch_profiles WHERE run_id = ?;", (run_id,))
    conn.execute("DELETE FROM alerts WHERE run_id = ?;", (run_id,))
    conn.execute("DELETE FROM run_log WHERE run_id = ?;", (run_id,))
