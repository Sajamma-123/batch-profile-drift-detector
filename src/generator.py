"""Sample data generator with deterministic seeding for normal, drifted, and corrupted batches."""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd


def generate_transactions(
    n_rows: int = 100,
    seed: int = 42,
    scenario: str = "baseline",
    start_time: Optional[datetime] = None,
    output_path: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    Generate synthetic transactions data.

    Scenarios:
      - 'baseline': Clean batch, normal distributions, null rate < 1%, valid keys.
      - 'normal': Follows baseline distribution closely (clean happy path).
      - 'drift_nulls': High null rate in customer_id (e.g. 18% vs baseline 1%).
      - 'drift_amount': Shifted amount mean (e.g. 3x normal average).
      - 'drift_freshness': Outdated timestamps (e.g. 30 days old).
      - 'corrupted': Mix of malformed records, duplicate keys, null required fields, bad FKs.
      - 'schema_change': Extra columns or modified schema.
    """
    rng = random.Random(seed)
    if start_time is None:
        start_time = datetime(2026, 10, 20, 8, 0, 0, tzinfo=timezone.utc)

    # Valid customer pool (CUST_0001 to CUST_0500)
    valid_customers = [f"CUST_{i:04d}" for i in range(1, 501)]
    merchants = [f"MERCH_{i:02d}" for i in range(1, 26)]
    currencies = ["USD", "EUR", "GBP", "CAD"]
    currency_weights = [0.75, 0.15, 0.07, 0.03]
    statuses = ["COMPLETED", "PENDING", "FAILED"]
    status_weights = [0.85, 0.10, 0.05]

    records: List[Dict] = []

    # Scenario parameters
    null_customer_rate = 0.01
    amount_mean = 120.0
    amount_std = 45.0
    time_offset_days = 0

    if scenario == "drift_nulls":
        null_customer_rate = 0.18
    elif scenario == "drift_amount":
        amount_mean = 350.0
        amount_std = 120.0
    elif scenario == "drift_freshness":
        time_offset_days = -30  # 30 days in past

    effective_start = start_time + timedelta(days=time_offset_days)

    for i in range(1, n_rows + 1):
        tx_id = f"TX_{seed:04d}_{i:06d}"
        
        # Customer ID with potential nulls
        if rng.random() < null_customer_rate:
            cust_id = None
        else:
            cust_id = rng.choice(valid_customers)

        # Amount
        amt = round(max(5.0, rng.gauss(amount_mean, amount_std)), 2)

        curr = rng.choices(currencies, weights=currency_weights, k=1)[0]
        stat = rng.choices(statuses, weights=status_weights, k=1)[0]
        
        # Timestamp advancing by random seconds
        tx_time = effective_start + timedelta(seconds=i * rng.randint(5, 30))
        tx_time_str = tx_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        merch = rng.choice(merchants)

        records.append({
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "amount": amt,
            "currency": curr,
            "status": stat,
            "timestamp": tx_time_str,
            "merchant_id": merch,
        })

    # Inject specific corruption for 'corrupted' scenario
    if scenario == "corrupted" and len(records) >= 6:
        # 1. Malformed amount (non-numeric string)
        records[0]["amount"] = "INVALID_AMT"
        
        # 2. Duplicate primary key
        records[1]["transaction_id"] = records[2]["transaction_id"]
        
        # 3. Null required field (timestamp is None)
        records[3]["timestamp"] = None
        
        # 4. Null required field (currency is None)
        records[4]["currency"] = None
        
        # 5. Invalid foreign reference key (not in reference table)
        records[5]["customer_id"] = "CUST_9999"

    df = pd.DataFrame(records)

    if scenario == "schema_change":
        # Add unexpected column and drop a non-required one
        df["loyalty_tier"] = "PLATINUM"
        df["fraud_score"] = 0.02

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction batches.")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument(
        "--scenario",
        type=str,
        default="baseline",
        choices=["baseline", "normal", "drift_nulls", "drift_amount", "drift_freshness", "corrupted", "schema_change"],
        help="Generation scenario.",
    )
    parser.add_argument("--output", type=str, required=True, help="Output CSV path.")

    args = parser.parse_args()
    df_gen = generate_transactions(
        n_rows=args.rows,
        seed=args.seed,
        scenario=args.scenario,
        output_path=args.output,
    )
    print(f"Generated {len(df_gen)} rows for scenario '{args.scenario}' saved to {args.output}")
