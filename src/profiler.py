"""Statistical profiling engine for baseline and incoming batches."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd


def compute_profile(
    df: pd.DataFrame,
    key_columns: Optional[List[str]] = None,
    top_n: int = 5,
) -> Dict[str, Any]:
    """
    Compute statistical profile for a DataFrame:
      - row count
      - min / max timestamp (freshness)
      - for each column:
          - null count & null rate
          - distinct count
          - min, max, mean, stddev (numeric)
          - top values and frequencies (categorical / text)
    """
    total_rows = len(df)
    if total_rows == 0:
        return {
            "row_count": 0,
            "min_timestamp": None,
            "max_timestamp": None,
            "columns": {},
        }

    cols = key_columns if key_columns is not None else list(df.columns)
    columns_profile: Dict[str, Any] = {}

    min_ts: Optional[str] = None
    max_ts: Optional[str] = None

    if "timestamp" in df.columns:
        ts_series = df["timestamp"].dropna().astype(str)
        if not ts_series.empty:
            min_ts = ts_series.min()
            max_ts = ts_series.max()

    for col in cols:
        if col not in df.columns:
            continue

        series = df[col]
        null_count = int(series.isna().sum())
        null_rate = round(null_count / total_rows, 4)
        non_null_series = series.dropna()
        distinct_count = int(non_null_series.nunique())

        col_meta: Dict[str, Any] = {
            "null_count": null_count,
            "null_rate": null_rate,
            "distinct_count": distinct_count,
        }

        # Check if numeric
        numeric_series = pd.to_numeric(non_null_series, errors="coerce")
        if numeric_series.notna().sum() > 0 and (
            pd.api.types.is_numeric_dtype(series.dtype) or col in ("amount", "price", "count")
        ):
            valid_nums = numeric_series.dropna()
            col_meta["type"] = "numeric"
            col_meta["min"] = round(float(valid_nums.min()), 4) if not valid_nums.empty else None
            col_meta["max"] = round(float(valid_nums.max()), 4) if not valid_nums.empty else None
            col_meta["mean"] = round(float(valid_nums.mean()), 4) if not valid_nums.empty else None
            col_meta["std"] = round(float(valid_nums.std()), 4) if len(valid_nums) > 1 else 0.0
        else:
            col_meta["type"] = "string/categorical"
            # Top N value frequencies
            if not non_null_series.empty:
                val_counts = non_null_series.astype(str).value_counts().head(top_n).to_dict()
                col_meta["top_values"] = {k: int(v) for k, v in val_counts.items()}
            else:
                col_meta["top_values"] = {}

        columns_profile[col] = col_meta

    return {
        "row_count": total_rows,
        "min_timestamp": min_ts,
        "max_timestamp": max_ts,
        "columns": columns_profile,
    }


def save_baseline_profile(
    conn: sqlite3.Connection,
    profile_id: str,
    profile_data: Dict[str, Any],
) -> None:
    """Save or update baseline profile in SQLite."""
    now_iso = datetime.now(timezone.utc).isoformat()
    metrics_json = json.dumps(profile_data, indent=2)
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO baseline_profiles 
            (profile_id, created_at, row_count, min_timestamp, max_timestamp, metrics_json)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                profile_id,
                now_iso,
                profile_data["row_count"],
                profile_data.get("min_timestamp"),
                profile_data.get("max_timestamp"),
                metrics_json,
            ),
        )


def load_baseline_profile(
    conn: sqlite3.Connection,
    profile_id: str = "default_baseline",
) -> Optional[Dict[str, Any]]:
    """Load baseline profile from SQLite."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT metrics_json FROM baseline_profiles WHERE profile_id = ? ORDER BY created_at DESC LIMIT 1;",
        (profile_id,),
    )
    row = cursor.fetchone()
    if row:
        return json.loads(row[0])
    return None


def save_batch_profile(
    conn: sqlite3.Connection,
    run_id: str,
    profile_id: str,
    profile_data: Dict[str, Any],
) -> None:
    """Save batch profile for the current run."""
    now_iso = datetime.now(timezone.utc).isoformat()
    metrics_json = json.dumps(profile_data, indent=2)
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO batch_profiles 
            (run_id, profile_id, created_at, row_count, min_timestamp, max_timestamp, metrics_json)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                run_id,
                profile_id,
                now_iso,
                profile_data["row_count"],
                profile_data.get("min_timestamp"),
                profile_data.get("max_timestamp"),
                metrics_json,
            ),
        )
