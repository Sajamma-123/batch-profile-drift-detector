"""Batch comparator: compares incoming batch profile against baseline profile to compute deltas."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


def parse_iso(ts_str: Optional[str]) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        clean = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(clean)
    except Exception:
        return None


def compare_profiles(
    baseline: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compare baseline profile and current batch profile.
    Computes deltas for row counts, freshness timestamps, and column-level metrics.
    """
    base_rows = baseline.get("row_count", 0)
    curr_rows = current.get("row_count", 0)
    row_delta = curr_rows - base_rows
    row_pct_change = round((row_delta / base_rows), 4) if base_rows > 0 else 0.0

    # Freshness comparison (max timestamp)
    base_max_ts_str = baseline.get("max_timestamp")
    curr_max_ts_str = current.get("max_timestamp")
    base_max_dt = parse_iso(base_max_ts_str)
    curr_max_dt = parse_iso(curr_max_ts_str)

    freshness_delta_days: Optional[float] = None
    if base_max_dt and curr_max_dt:
        # positive if current is newer; negative if current is older than baseline
        diff_seconds = (curr_max_dt - base_max_dt).total_seconds()
        freshness_delta_days = round(diff_seconds / 86400.0, 2)

    # Column-level comparisons
    base_cols = baseline.get("columns", {})
    curr_cols = current.get("columns", {})
    all_col_names = sorted(set(base_cols.keys()).union(set(curr_cols.keys())))

    column_deltas: Dict[str, Any] = {}

    for col in all_col_names:
        b_meta = base_cols.get(col, {})
        c_meta = curr_cols.get(col, {})

        # Null rate delta
        b_null_rate = b_meta.get("null_rate", 0.0)
        c_null_rate = c_meta.get("null_rate", 0.0)
        null_delta = round(c_null_rate - b_null_rate, 4)

        # Distinct count delta
        b_distinct = b_meta.get("distinct_count", 0)
        c_distinct = c_meta.get("distinct_count", 0)
        distinct_delta = c_distinct - b_distinct
        distinct_pct_change = (
            round((distinct_delta / b_distinct), 4) if b_distinct > 0 else 0.0
        )

        col_diff: Dict[str, Any] = {
            "null_rate": {
                "baseline": b_null_rate,
                "current": c_null_rate,
                "delta": null_delta,
            },
            "distinct_count": {
                "baseline": b_distinct,
                "current": c_distinct,
                "delta": distinct_delta,
                "pct_change": distinct_pct_change,
            },
        }

        # Numeric mean & ranges
        if b_meta.get("type") == "numeric" or c_meta.get("type") == "numeric":
            b_mean = b_meta.get("mean")
            c_mean = c_meta.get("mean")
            if b_mean is not None and c_mean is not None:
                mean_delta = round(c_mean - b_mean, 4)
                mean_pct_change = round((mean_delta / b_mean), 4) if b_mean != 0 else 0.0
                col_diff["mean"] = {
                    "baseline": b_mean,
                    "current": c_mean,
                    "delta": mean_delta,
                    "pct_change": mean_pct_change,
                }
            if b_meta.get("min") is not None and c_meta.get("min") is not None:
                col_diff["min"] = {
                    "baseline": b_meta.get("min"),
                    "current": c_meta.get("min"),
                    "delta": round(c_meta.get("min") - b_meta.get("min"), 4),
                }
            if b_meta.get("max") is not None and c_meta.get("max") is not None:
                col_diff["max"] = {
                    "baseline": b_meta.get("max"),
                    "current": c_meta.get("max"),
                    "delta": round(c_meta.get("max") - b_meta.get("max"), 4),
                }

        column_deltas[col] = col_diff

    return {
        "row_count": {
            "baseline": base_rows,
            "current": curr_rows,
            "delta": row_delta,
            "pct_change": row_pct_change,
        },
        "freshness": {
            "baseline_max_ts": base_max_ts_str,
            "current_max_ts": curr_max_ts_str,
            "delta_days": freshness_delta_days,
        },
        "columns": column_deltas,
    }
