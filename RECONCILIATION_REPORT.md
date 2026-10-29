# Data Pipeline Reconciliation & Integrity Report

Reconciliation is the fundamental verification that guarantees zero silent data loss. Every incoming row received by the pipeline must be accounted for either in the validated analytical target store (`transactions`) or in the audit quarantine store (`quarantine`).

---

## 1. The Core Reconciliation Formula

$$\text{Source Input Rows} = \text{Target Valid Rows} + \text{Quarantined Rows}$$

$$\text{Discrepancy (Diff)} = \text{Source Input Rows} - (\text{Target Valid Rows} + \text{Quarantined Rows}) = 0$$

If $\text{Diff} \ne 0$, an investigation is immediately triggered for silent data drops, unhandled exceptions, or deduplication anomalies.

---

## 2. Standard Reconciliation SQL Queries

You can execute these SQL queries directly against `pipeline.db` using the SQLite CLI or Python SQLite:

### Query 1: Run-Level Master Reconciliation
```sql
SELECT 
    r.run_id,
    r.source_file,
    r.status,
    r.input_count AS source_rows,
    (SELECT COUNT(*) FROM transactions t WHERE t.run_id = r.run_id) AS target_valid_rows,
    (SELECT COUNT(DISTINCT transaction_id) FROM transactions t WHERE t.run_id = r.run_id) AS target_distinct_rows,
    (SELECT COUNT(*) FROM quarantine q WHERE q.run_id = r.run_id) AS quarantined_rows,
    ((SELECT COUNT(*) FROM transactions t WHERE t.run_id = r.run_id) + 
     (SELECT COUNT(*) FROM quarantine q WHERE q.run_id = r.run_id)) AS total_accounted,
    (r.input_count - 
     ((SELECT COUNT(*) FROM transactions t WHERE t.run_id = r.run_id) + 
      (SELECT COUNT(*) FROM quarantine q WHERE q.run_id = r.run_id))) AS discrepancy_diff
FROM run_log r
ORDER BY r.start_time DESC;
```

### Query 2: Quarantine Root Cause Breakdown
```sql
SELECT 
    run_id,
    quarantine_reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM quarantine q2 WHERE q2.run_id = q.run_id), 2) AS pct_of_rejections
FROM quarantine q
GROUP BY run_id, quarantine_reason
ORDER BY run_id DESC, rejected_count DESC;
```

### Query 3: Target Duplicate Verification
```sql
SELECT 
    run_id,
    transaction_id,
    COUNT(*) AS duplicate_count
FROM transactions
GROUP BY run_id, transaction_id
HAVING COUNT(*) > 1;
-- Expected output: 0 rows (zero duplicates in target store)
```

---

## 3. Automated Reconciliation CLI Output

Run the reconciliation engine anytime with:
```bash
python pipeline.py --reconcile
```

### Sample CLI Output:
```text
==============================================================================
DATA PIPELINE RECONCILIATION & INTEGRITY REPORT
==============================================================================

Run ID: run_006  [OK]
  Source File:       data/batch_006_corrupted.csv
  Input Count:       100 rows
  Target Loaded:     94 rows (distinct: 94)
  Quarantined:       6 rows
  Total Accounted:   100 rows
  Discrepancy (Diff):0 rows
  Status:            Source records completely reconciled (100% accounted for).
  Quarantine Reasons Breakdown:
    - [1 rows] NULL_REQUIRED_FIELD: 'timestamp' is null or empty
    - [1 rows] NULL_REQUIRED_FIELD: 'customer_id' is null or empty
    - [1 rows] NULL_REQUIRED_FIELD: 'currency' is null or empty
    - [1 rows] MALFORMED_RECORD: 'amount' value 'INVALID_AMT' is not a valid number
    - [1 rows] INVALID_REFERENCE_KEY: customer_id='CUST_9999' does not exist in customers
    - [1 rows] DUPLICATE_KEY_INTRA_BATCH: transaction_id='TX_0047_000003' already seen in this batch

Run ID: run_003  [OK]
  Source File:       data/batch_003_drift_nulls.csv
  Input Count:       100 rows
  Target Loaded:     72 rows (distinct: 72)
  Quarantined:       28 rows
  Total Accounted:   100 rows
  Discrepancy (Diff):0 rows
  Status:            Source records completely reconciled (100% accounted for).
  Quarantine Reasons Breakdown:
    - [28 rows] NULL_REQUIRED_FIELD: 'customer_id' is null or empty

Run ID: run_001  [OK]
  Source File:       data/batch_001_baseline.csv
  Input Count:       100 rows
  Target Loaded:     99 rows (distinct: 99)
  Quarantined:       1 rows
  Total Accounted:   100 rows
  Discrepancy (Diff):0 rows
  Status:            Source records completely reconciled (100% accounted for).
  Quarantine Reasons Breakdown:
    - [1 rows] NULL_REQUIRED_FIELD: 'customer_id' is null or empty

==============================================================================
Overall Result: [PASSED] ALL RUNS FULLY RECONCILED
==============================================================================
```

---

## 4. Idempotency Verification

When `run_006` is executed multiple times with identical or updated input files:
1. Target records previously loaded under `run_id = 'run_006'` are atomically purged before re-insertion.
2. Quarantined records under `run_id = 'run_006'` are replaced.
3. Target count remains exactly equal to the latest valid row count (no duplication occurs).
4. `run_log` metrics are refreshed with the latest execution timestamps and counts.
