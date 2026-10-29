"""Reconciliation engine: verifies source count == target count + quarantine count and explains diffs."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional


def run_reconciliation(
    conn: sqlite3.Connection,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Perform reconciliation check between run_log, target transactions table, and quarantine table.
    """
    cursor = conn.cursor()

    if run_id:
        cursor.execute("SELECT * FROM run_log WHERE run_id = ?;", (run_id,))
    else:
        cursor.execute("SELECT * FROM run_log ORDER BY start_time DESC LIMIT 10;")
    
    run_rows = cursor.fetchall()
    reconciliation_results: List[Dict[str, Any]] = []

    for r in run_rows:
        rid = r["run_id"]
        input_count = r["input_count"]
        logged_output = r["output_count"]
        logged_rejected = r["rejected_count"]

        # Actual count in transactions table for this run_id
        cursor.execute(
            "SELECT COUNT(*) as cnt, COUNT(DISTINCT transaction_id) as dist_cnt FROM transactions WHERE run_id = ?;",
            (rid,),
        )
        t_row = cursor.fetchone()
        actual_target_count = t_row["cnt"]
        distinct_target_count = t_row["dist_cnt"]

        # Actual count in quarantine table for this run_id
        cursor.execute("SELECT COUNT(*) as cnt FROM quarantine WHERE run_id = ?;", (rid,))
        actual_quarantine_count = cursor.fetchone()["cnt"]

        # Quarantine breakdown by reason
        cursor.execute(
            """
            SELECT quarantine_reason, COUNT(*) as cnt 
            FROM quarantine 
            WHERE run_id = ? 
            GROUP BY quarantine_reason 
            ORDER BY cnt DESC;
            """,
            (rid,),
        )
        reason_rows = cursor.fetchall()
        reasons_breakdown = {row["quarantine_reason"]: row["cnt"] for row in reason_rows}

        total_accounted = actual_target_count + actual_quarantine_count
        diff = input_count - total_accounted
        is_reconciled = (diff == 0) and (actual_target_count == distinct_target_count)

        explanation = (
            "Source records completely reconciled (100% accounted for)."
            if is_reconciled
            else f"Unreconciled difference of {diff} rows! (Target duplicates: {actual_target_count - distinct_target_count})"
        )

        reconciliation_results.append({
            "run_id": rid,
            "source_file": r["source_file"],
            "status": r["status"],
            "input_count": input_count,
            "target_count": actual_target_count,
            "distinct_target_count": distinct_target_count,
            "quarantine_count": actual_quarantine_count,
            "total_accounted": total_accounted,
            "diff": diff,
            "is_reconciled": is_reconciled,
            "explanation": explanation,
            "quarantine_reasons": reasons_breakdown,
        })

    return {
        "runs": reconciliation_results,
        "all_passed": all(res["is_reconciled"] for res in reconciliation_results) if reconciliation_results else True,
    }


def format_reconciliation_report(reconciliation_data: Dict[str, Any]) -> str:
    """Format reconciliation results as a human-readable summary report."""
    lines: List[str] = []
    lines.append("=" * 78)
    lines.append("DATA PIPELINE RECONCILIATION & INTEGRITY REPORT")
    lines.append("=" * 78)

    for run in reconciliation_data.get("runs", []):
        status_symbol = "[OK]" if run["is_reconciled"] else "[DIFF]"
        lines.append(f"\nRun ID: {run['run_id']}  {status_symbol}")
        lines.append(f"  Source File:       {run['source_file']}")
        lines.append(f"  Input Count:       {run['input_count']} rows")
        lines.append(f"  Target Loaded:     {run['target_count']} rows (distinct: {run['distinct_target_count']})")
        lines.append(f"  Quarantined:       {run['quarantine_count']} rows")
        lines.append(f"  Total Accounted:   {run['total_accounted']} rows")
        lines.append(f"  Discrepancy (Diff):{run['diff']} rows")
        lines.append(f"  Status:            {run['explanation']}")

        if run["quarantine_reasons"]:
            lines.append("  Quarantine Reasons Breakdown:")
            for reason, cnt in run["quarantine_reasons"].items():
                lines.append(f"    - [{cnt} rows] {reason}")

    lines.append("\n" + "=" * 78)
    overall = "[PASSED] ALL RUNS FULLY RECONCILED" if reconciliation_data.get("all_passed") else "[FAILED] RECONCILIATION DISCREPANCY DETECTED"
    lines.append(f"Overall Result: {overall}")
    lines.append("=" * 78)

    return "\n".join(lines)
