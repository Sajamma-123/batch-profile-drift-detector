"""Explainable Alert Engine: detects threshold breaches and generates structured human-readable alerts."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class Alert:
    metric: str
    column: str
    severity: str
    baseline_value: float
    current_value: float
    delta: float
    threshold: float
    message: str

    def format_alert(self) -> str:
        """
        Format alert matching the required human-readable specification:
        ALERT [null_rate] column=customer_id severity=HIGH
          baseline=0.02  current=0.18  delta=+0.16  threshold=0.10
          message: "Null rate increased 16 points vs baseline"
        """
        delta_sign = "+" if self.delta > 0 else ""
        return (
            f"ALERT [{self.metric}] column={self.column} severity={self.severity}\n"
            f"  baseline={self.baseline_value:.2f}  current={self.current_value:.2f}  "
            f"delta={delta_sign}{self.delta:.2f}  threshold={self.threshold:.2f}\n"
            f'  message: "{self.message}"'
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric": self.metric,
            "column": self.column,
            "severity": self.severity,
            "baseline_value": self.baseline_value,
            "current_value": self.current_value,
            "delta": self.delta,
            "threshold": self.threshold,
            "message": self.message,
        }


def evaluate_alerts(
    deltas: Dict[str, Any],
    config: Dict[str, Any],
) -> List[Alert]:
    """
    Evaluate profile deltas against config thresholds and generate explainable alerts.
    """
    thresholds = config.get("thresholds", {})
    null_rate_thresh = float(thresholds.get("null_rate_delta", 0.10))
    row_drop_thresh = float(thresholds.get("row_count_drop_pct", 0.25))
    row_surge_thresh = float(thresholds.get("row_count_surge_pct", 1.00))
    mean_shift_thresh = float(thresholds.get("mean_shift_pct", 0.20))
    freshness_lag_thresh = float(thresholds.get("freshness_lag_days", 3.0))
    distinct_drop_thresh = float(thresholds.get("distinct_count_drop_pct", 0.30))

    alerts: List[Alert] = []

    # 1. Row count checks
    row_meta = deltas.get("row_count", {})
    base_rows = float(row_meta.get("baseline", 0))
    curr_rows = float(row_meta.get("current", 0))
    row_pct_change = float(row_meta.get("pct_change", 0.0))

    if base_rows > 0:
        # Check volume drop
        if row_pct_change < -row_drop_thresh:
            drop_pts = abs(round(row_pct_change * 100))
            alerts.append(
                Alert(
                    metric="row_count",
                    column="_batch_",
                    severity="HIGH",
                    baseline_value=base_rows,
                    current_value=curr_rows,
                    delta=float(row_meta.get("delta", 0)),
                    threshold=base_rows * (1.0 - row_drop_thresh),
                    message=f"Row count dropped {drop_pts}% vs baseline ({int(base_rows)} -> {int(curr_rows)})",
                )
            )
        # Check volume surge
        elif row_pct_change > row_surge_thresh:
            surge_pts = round(row_pct_change * 100)
            alerts.append(
                Alert(
                    metric="row_count",
                    column="_batch_",
                    severity="MEDIUM",
                    baseline_value=base_rows,
                    current_value=curr_rows,
                    delta=float(row_meta.get("delta", 0)),
                    threshold=base_rows * (1.0 + row_surge_thresh),
                    message=f"Row count surged {surge_pts}% vs baseline ({int(base_rows)} -> {int(curr_rows)})",
                )
            )

    # 2. Freshness check
    freshness = deltas.get("freshness", {})
    delta_days = freshness.get("delta_days")
    if delta_days is not None:
        # If current max timestamp is older than baseline by more than lag threshold
        if delta_days < -freshness_lag_thresh:
            lag = abs(delta_days)
            alerts.append(
                Alert(
                    metric="freshness_lag",
                    column="timestamp",
                    severity="HIGH",
                    baseline_value=0.0,
                    current_value=lag,
                    delta=lag,
                    threshold=freshness_lag_thresh,
                    message=f"Data is out-of-order or stale by {lag:.1f} days vs baseline timestamp",
                )
            )

    # 3. Column-level checks
    col_deltas = deltas.get("columns", {})
    for col_name, col_info in col_deltas.items():
        # Null rate alert
        null_info = col_info.get("null_rate", {})
        null_delta = float(null_info.get("delta", 0.0))
        b_null = float(null_info.get("baseline", 0.0))
        c_null = float(null_info.get("current", 0.0))

        if null_delta >= null_rate_thresh:
            points = round(null_delta * 100)
            alerts.append(
                Alert(
                    metric="null_rate",
                    column=col_name,
                    severity="HIGH",
                    baseline_value=b_null,
                    current_value=c_null,
                    delta=null_delta,
                    threshold=null_rate_thresh,
                    message=f"Null rate increased {points} points vs baseline",
                )
            )

        # Numeric mean shift alert
        if "mean" in col_info:
            mean_info = col_info["mean"]
            b_mean = float(mean_info.get("baseline", 0.0))
            c_mean = float(mean_info.get("current", 0.0))
            mean_delta = float(mean_info.get("delta", 0.0))
            mean_pct = float(mean_info.get("pct_change", 0.0))

            if abs(mean_pct) >= mean_shift_thresh:
                shift_pts = round(abs(mean_pct) * 100)
                direction = "increased" if mean_pct > 0 else "decreased"
                alerts.append(
                    Alert(
                        metric="mean_shift",
                        column=col_name,
                        severity="HIGH",
                        baseline_value=b_mean,
                        current_value=c_mean,
                        delta=mean_delta,
                        threshold=round(b_mean * mean_shift_thresh, 2),
                        message=f"Mean for {col_name} {direction} by {shift_pts}% vs baseline",
                    )
                )

        # Distinct count drop alert
        if "distinct_count" in col_info:
            dist_info = col_info["distinct_count"]
            b_dist = float(dist_info.get("baseline", 0))
            c_dist = float(dist_info.get("current", 0))
            dist_delta = float(dist_info.get("delta", 0))
            dist_pct = float(dist_info.get("pct_change", 0.0))

            if dist_pct < -distinct_drop_thresh and b_dist > 5:
                drop_pct = round(abs(dist_pct) * 100)
                alerts.append(
                    Alert(
                        metric="distinct_count_drop",
                        column=col_name,
                        severity="MEDIUM",
                        baseline_value=b_dist,
                        current_value=c_dist,
                        delta=dist_delta,
                        threshold=round(b_dist * (1.0 - distinct_drop_thresh), 2),
                        message=f"Distinct count for {col_name} dropped {drop_pct}% vs baseline ({int(b_dist)} -> {int(c_dist)})",
                    )
                )

    return alerts


def save_alerts(
    conn: sqlite3.Connection,
    run_id: str,
    alerts: List[Alert],
) -> None:
    """Save alert records to SQLite alerts table."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        for a in alerts:
            conn.execute(
                """
                INSERT INTO alerts 
                (run_id, metric, column_name, severity, baseline_value, current_value, delta, threshold, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    run_id,
                    a.metric,
                    a.column,
                    a.severity,
                    a.baseline_value,
                    a.current_value,
                    a.delta,
                    a.threshold,
                    a.message,
                    now_iso,
                ),
            )
