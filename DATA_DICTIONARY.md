# Data Dictionary

This document defines the schema, data types, keys, constraints, nullability, and descriptions for all tables managed within the pipeline database (`pipeline.db`).

---

## 1. `transactions`
**Description:** Target analytical store containing validated and ingested financial transaction records.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `transaction_id` | `TEXT` | PK | **NO** | Unique transaction identifier | `"TX_0042_000001"` |
| `customer_id` | `TEXT` | FK | **NO** | References `customers(customer_id)` | `"CUST_0014"` |
| `amount` | `REAL` | None | **NO** | Transaction monetary value in standard currency units (must be >= 0.0) | `142.50` |
| `currency` | `TEXT` | None | **NO** | ISO 4217 currency code (`USD`, `EUR`, `GBP`, `CAD`, `JPY`) | `"USD"` |
| `status` | `TEXT` | None | **NO** | Processing state (`COMPLETED`, `PENDING`, `FAILED`, `CANCELLED`) | `"COMPLETED"` |
| `timestamp` | `TEXT` | None | **NO** | ISO 8601 UTC timestamp of transaction occurrence | `"2026-10-25T10:15:30Z"` |
| `merchant_id` | `TEXT` | None | YES | Identifier of merchant/vendor | `"MERCH_04"` |
| `run_id` | `TEXT` | None | **NO** | Identifier of pipeline execution run that loaded this record | `"run_002"` |
| `ingested_at` | `TEXT` | None | **NO** | ISO 8601 UTC timestamp when row was written to database | `"2026-10-29T12:00:00Z"` |

---

## 2. `customers`
**Description:** Reference dimension table used for foreign key integrity validation during ingestion.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `customer_id` | `TEXT` | PK | **NO** | Primary customer identification key | `"CUST_0001"` |
| `name` | `TEXT` | None | **NO** | Full name or business entity name | `"Customer 1"` |
| `created_at` | `TEXT` | None | **NO** | Customer account opening timestamp | `"2026-01-01T00:00:00Z"` |

---

## 3. `quarantine`
**Description:** Audit trail of all rejected records that failed schema checks, domain ranges, duplicate keys, or referential integrity.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `quarantine_id` | `INTEGER` | PK | **NO** | Auto-incrementing surrogate primary key | `1` |
| `run_id` | `TEXT` | None | **NO** | Pipeline run ID during which rejection occurred | `"run_006"` |
| `source_file` | `TEXT` | None | YES | Path or filename of the incoming batch | `"data/batch_006_corrupted.csv"` |
| `raw_record` | `TEXT` | None | **NO** | Verbatim raw row serialized as JSON | `'{"transaction_id": "TX_99", ...}'` |
| `quarantine_reason` | `TEXT` | None | **NO** | Structured human-readable explanation of rejection cause(s) | `"MALFORMED_RECORD: 'amount' value 'INVALID_AMT' is not a valid number"` |
| `quarantined_at` | `TEXT` | None | **NO** | ISO 8601 UTC timestamp when row was quarantined | `"2026-10-29T12:05:00Z"` |

---

## 4. `baseline_profiles`
**Description:** Stores established baseline statistical profiles against which future batches are compared.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `profile_id` | `TEXT` | PK | **NO** | Identifier name of baseline (e.g. `default_baseline`) | `"default_baseline"` |
| `created_at` | `TEXT` | None | **NO** | Timestamp when baseline was computed and stored | `"2026-10-29T12:00:00Z"` |
| `row_count` | `INTEGER` | None | **NO** | Total row count in baseline batch | `100` |
| `min_timestamp` | `TEXT` | None | YES | Earliest transaction timestamp in baseline | `"2026-10-20T08:00:15Z"` |
| `max_timestamp` | `TEXT` | None | YES | Latest transaction timestamp in baseline (freshness reference) | `"2026-10-20T08:24:45Z"` |
| `metrics_json` | `TEXT` | None | **NO** | JSON object storing per-column null rates, distinct counts, numeric distributions (min/max/mean/std), and top category counts | `'{"row_count": 100, ...}'` |

---

## 5. `batch_profiles`
**Description:** Stores historical statistical profiles computed for every batch processed by the pipeline.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `run_id` | `TEXT` | PK | **NO** | Pipeline run ID | `"run_003"` |
| `profile_id` | `TEXT` | None | YES | Baseline profile compared against | `"default_baseline"` |
| `created_at` | `TEXT` | None | **NO** | Timestamp when profile was recorded | `"2026-10-29T12:02:00Z"` |
| `row_count` | `INTEGER` | None | **NO** | Total row count in incoming batch | `100` |
| `min_timestamp` | `TEXT` | None | YES | Earliest transaction timestamp in batch | `"2026-10-20T08:00:15Z"` |
| `max_timestamp` | `TEXT` | None | YES | Latest transaction timestamp in batch | `"2026-10-20T08:24:45Z"` |
| `metrics_json` | `TEXT` | None | **NO** | JSON payload containing all column-level statistics | `'{"row_count": 100, ...}'` |

---

## 6. `alerts`
**Description:** Explainable alert log recording threshold breaches and drift detections.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `alert_id` | `INTEGER` | PK | **NO** | Auto-incrementing alert identifier | `1` |
| `run_id` | `TEXT` | None | **NO** | Associated pipeline run ID | `"run_003"` |
| `metric` | `TEXT` | None | **NO** | Name of breached metric (`null_rate`, `mean_shift`, `row_count`, `freshness_lag`, `distinct_count_drop`) | `"null_rate"` |
| `column_name` | `TEXT` | None | **NO** | Name of impacted column (or `_batch_` for batch-level metrics) | `"customer_id"` |
| `severity` | `TEXT` | None | **NO** | Alert severity (`INFO`, `MEDIUM`, `HIGH`, `CRITICAL`) | `"HIGH"` |
| `baseline_value` | `REAL` | None | YES | Metric value observed in baseline | `0.02` |
| `current_value` | `REAL` | None | YES | Metric value observed in current batch | `0.18` |
| `delta` | `REAL` | None | YES | Difference between current and baseline (`current - baseline`) | `0.16` |
| `threshold` | `REAL` | None | YES | Configured alert threshold value | `0.10` |
| `message` | `TEXT` | None | **NO** | Human-readable explanation of why alert fired | `"Null rate increased 16 points vs baseline"` |
| `created_at` | `TEXT` | None | **NO** | Timestamp when alert was logged | `"2026-10-29T12:02:00Z"` |

---

## 7. `run_log`
**Description:** Execution registry tracking input/output counts, status, and reconciliation metrics for every run.

| Column | Type | Key | Nullable | Description | Example |
|---|---|---|---|---|---|
| `run_id` | `TEXT` | PK | **NO** | Unique pipeline execution run identifier | `"run_002"` |
| `source_file` | `TEXT` | None | YES | Filepath or URI of the input batch | `"data/batch_002_normal.csv"` |
| `start_time` | `TEXT` | None | **NO** | ISO 8601 UTC timestamp of execution start | `"2026-10-29T12:00:00Z"` |
| `end_time` | `TEXT` | None | YES | ISO 8601 UTC timestamp of execution completion | `"2026-10-29T12:00:01Z"` |
| `status` | `TEXT` | None | **NO** | Final run state (`SUCCESS`, `COMPLETED_WITH_ALERTS`, `ALL_QUARANTINED`, `REJECTED_SCHEMA`, `FAILED`) | `"SUCCESS"` |
| `input_count` | `INTEGER` | None | **NO** | Total row count in source batch file | `100` |
| `output_count` | `INTEGER` | None | **NO** | Valid rows successfully loaded into `transactions` | `98` |
| `rejected_count`| `INTEGER` | None | **NO** | Invalid rows routed to `quarantine` | `2` |
| `alerts_count` | `INTEGER` | None | **NO** | Count of explainable alerts generated | `0` |
| `failure_reason`| `TEXT` | None | YES | Error message if pipeline execution failed fatally | `None` |
