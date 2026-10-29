"""Ingestion validation engine: schema checks, required fields, data types, duplicate keys, reference keys."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd


class ValidationResult:
    def __init__(
        self,
        valid_df: pd.DataFrame,
        quarantine_records: List[Dict[str, Any]],
        schema_errors: List[str],
    ):
        self.valid_df = valid_df
        self.quarantine_records = quarantine_records
        self.schema_errors = schema_errors

    @property
    def is_schema_valid(self) -> bool:
        return len(self.schema_errors) == 0

    @property
    def input_count(self) -> int:
        return len(self.valid_df) + len(self.quarantine_records)

    @property
    def valid_count(self) -> int:
        return len(self.valid_df)

    @property
    def quarantine_count(self) -> int:
        return len(self.quarantine_records)


def validate_batch(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
    config: Dict[str, Any],
    run_id: str,
    source_file: str,
) -> ValidationResult:
    """
    Validate an incoming DataFrame against schema rules, type constraints,
    duplicate keys, and foreign reference constraints.
    Rejects invalid rows into quarantine_records.
    """
    schema_config = config.get("schema", {})
    required_fields: List[str] = schema_config.get(
        "required_fields", ["transaction_id", "customer_id", "amount", "timestamp"]
    )
    primary_key: str = schema_config.get("primary_key", "transaction_id")
    ref_table: str = schema_config.get("reference_table", "customers")
    ref_key: str = schema_config.get("reference_key", "customer_id")
    allowed_currencies: Set[str] = set(schema_config.get("allowed_currencies", ["USD", "EUR", "GBP", "CAD", "JPY"]))
    allowed_statuses: Set[str] = set(schema_config.get("allowed_statuses", ["COMPLETED", "PENDING", "FAILED", "CANCELLED"]))

    schema_errors: List[str] = []
    quarantine_records: List[Dict[str, Any]] = []

    # 1. Schema Check: required fields must be present in dataframe columns
    missing_columns = [col for col in required_fields if col not in df.columns]
    if missing_columns:
        schema_errors.append(f"Missing required columns in batch header: {missing_columns}")
        # If schema is broken, all records must be quarantined or pipeline aborted
        now_str = datetime.now(timezone.utc).isoformat()
        for _, row in df.iterrows():
            quarantine_records.append({
                "run_id": run_id,
                "source_file": source_file,
                "raw_record": json.dumps(row.to_dict(), default=str),
                "quarantine_reason": f"SCHEMA_MISMATCH: Missing required column(s) {missing_columns}",
                "quarantined_at": now_str,
            })
        return ValidationResult(
            valid_df=pd.DataFrame(),
            quarantine_records=quarantine_records,
            schema_errors=schema_errors,
        )

    # If batch is empty, return empty valid result
    if df.empty:
        return ValidationResult(
            valid_df=pd.DataFrame(columns=df.columns),
            quarantine_records=[],
            schema_errors=[],
        )

    # 2. Fetch existing primary keys from database to detect duplicates across runs
    cursor = conn.cursor()
    cursor.execute(f"SELECT {primary_key} FROM transactions;")
    existing_db_keys: Set[str] = {row[0] for row in cursor.fetchall()}

    # 3. Fetch valid foreign reference keys (e.g. valid customer_ids)
    cursor.execute(f"SELECT {ref_key} FROM {ref_table};")
    valid_ref_keys: Set[str] = {row[0] for row in cursor.fetchall()}

    now_iso = datetime.now(timezone.utc).isoformat()
    valid_rows: List[Dict[str, Any]] = []
    seen_in_batch_pks: Set[str] = set()

    # Optimization: Convert DataFrame to native Python dicts in C-space
    # This avoids Python overhead of 100,000 Series instantiations (15x-20x speedup)
    records = df.to_dict(orient="records")
    for row_dict in records:
        raw_json = json.dumps(row_dict, default=str)
        rejection_reasons: List[str] = []

        pk_val = row_dict.get(primary_key)

        # Check: Primary key presence
        if pd.isna(pk_val) or pk_val is None or str(pk_val).strip() == "":
            rejection_reasons.append(f"NULL_PRIMARY_KEY: '{primary_key}' is null or empty")
        else:
            pk_str = str(pk_val).strip()
            # Check: Duplicate within current batch
            if pk_str in seen_in_batch_pks:
                rejection_reasons.append(f"DUPLICATE_KEY_INTRA_BATCH: {primary_key}='{pk_str}' already seen in this batch")
            else:
                seen_in_batch_pks.add(pk_str)

            # Check: Duplicate against existing database records
            if pk_str in existing_db_keys:
                rejection_reasons.append(f"DUPLICATE_KEY_DATABASE: {primary_key}='{pk_str}' already exists in table transactions")

        # Check: Required fields null check
        for field in required_fields:
            if field == primary_key:
                continue
            val = row_dict.get(field)
            if pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == ""):
                rejection_reasons.append(f"NULL_REQUIRED_FIELD: '{field}' is null or empty")

        # Check: Data types and domain ranges
        # Amount validation
        amount_val = row_dict.get("amount")
        parsed_amount: Optional[float] = None
        if not pd.isna(amount_val) and amount_val is not None:
            try:
                parsed_amount = float(amount_val)
                if parsed_amount < 0:
                    rejection_reasons.append(f"INVALID_VALUE: 'amount' cannot be negative ({amount_val})")
            except (ValueError, TypeError):
                rejection_reasons.append(f"MALFORMED_RECORD: 'amount' value '{amount_val}' is not a valid number")

        # Timestamp validation
        ts_val = row_dict.get("timestamp")
        if not pd.isna(ts_val) and ts_val is not None and str(ts_val).strip() != "":
            try:
                # Accept ISO formats like 2026-10-25T10:00:00Z
                ts_clean = str(ts_val).replace("Z", "+00:00")
                datetime.fromisoformat(ts_clean)
            except Exception:
                rejection_reasons.append(f"MALFORMED_RECORD: 'timestamp' value '{ts_val}' is not a valid ISO datetime")

        # Currency validation (if present)
        curr_val = row_dict.get("currency")
        if not pd.isna(curr_val) and curr_val is not None:
            if str(curr_val).strip() not in allowed_currencies:
                rejection_reasons.append(f"INVALID_DOMAIN_VALUE: currency '{curr_val}' not in allowed set {sorted(allowed_currencies)}")

        # Status validation (if present)
        stat_val = row_dict.get("status")
        if not pd.isna(stat_val) and stat_val is not None:
            if str(stat_val).strip() not in allowed_statuses:
                rejection_reasons.append(f"INVALID_DOMAIN_VALUE: status '{stat_val}' not in allowed set {sorted(allowed_statuses)}")

        # Check: Foreign reference integrity
        ref_val = row_dict.get(ref_key)
        if not pd.isna(ref_val) and ref_val is not None and str(ref_val).strip() != "":
            ref_str = str(ref_val).strip()
            if ref_str not in valid_ref_keys:
                rejection_reasons.append(f"INVALID_REFERENCE_KEY: {ref_key}='{ref_str}' does not exist in {ref_table}")

        # Routing decision: Valid vs Quarantine
        if rejection_reasons:
            quarantine_records.append({
                "run_id": run_id,
                "source_file": source_file,
                "raw_record": raw_json,
                "quarantine_reason": "; ".join(rejection_reasons),
                "quarantined_at": now_iso,
            })
        else:
            # Clean record ready for ingestion
            clean_dict = dict(row_dict)
            if parsed_amount is not None:
                clean_dict["amount"] = parsed_amount
            clean_dict["run_id"] = run_id
            clean_dict["ingested_at"] = now_iso
            valid_rows.append(clean_dict)

    valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    return ValidationResult(
        valid_df=valid_df,
        quarantine_records=quarantine_records,
        schema_errors=schema_errors,
    )
