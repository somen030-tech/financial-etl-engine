
from pathlib import Path
import json

import pandas as pd


REQUIRED_COLUMNS = {
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "isFraud",
}


def inspect_schema(actual_columns, baseline_path):
    """
    Save the first schema as a baseline.
    On later runs, compare current columns with that baseline.
    """
    actual = set(actual_columns)
    baseline_file = Path(baseline_path)
    baseline_file.parent.mkdir(parents=True, exist_ok=True)

    if not baseline_file.exists():
        baseline_file.write_text(
            json.dumps(sorted(actual), indent=2),
            encoding="utf-8",
        )

        return {
            "status": "BASELINE_CREATED",
            "missing_columns": [],
            "unexpected_columns": [],
            "missing_required_columns": sorted(REQUIRED_COLUMNS - actual),
        }

    baseline = set(
        json.loads(baseline_file.read_text(encoding="utf-8"))
    )

    missing = sorted(baseline - actual)
    unexpected = sorted(actual - baseline)
    missing_required = sorted(REQUIRED_COLUMNS - actual)

    if missing_required:
        status = "FAIL"
    elif missing or unexpected:
        status = "DRIFT_DETECTED"
    else:
        status = "PASS"

    return {
        "status": status,
        "missing_columns": missing,
        "unexpected_columns": unexpected,
        "missing_required_columns": missing_required,
    }


def validate_transactions(df, seen_ids=None):
    """
    Split a DataFrame into accepted and rejected rows.

    seen_ids tracks IDs from earlier chunks when supplied.
    The first occurrence of an ID is not rejected as a duplicate.
    """
    if seen_ids is None:
        seen_ids = set()

    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    data = df.copy()
    errors = [[] for _ in range(len(data))]

    # Convert fields temporarily for validation.
    transaction_ids = pd.to_numeric(
        data["TransactionID"], errors="coerce"
    )
    timestamps = pd.to_numeric(
        data["TransactionDT"], errors="coerce"
    )
    amounts = pd.to_numeric(
        data["TransactionAmt"], errors="coerce"
    )
    fraud_labels = pd.to_numeric(
        data["isFraud"], errors="coerce"
    )

    ids_seen_in_this_chunk = set()

    for position, (_, row) in enumerate(data.iterrows()):
        transaction_id = transaction_ids.iloc[position]
        timestamp = timestamps.iloc[position]
        amount = amounts.iloc[position]
        fraud_label = fraud_labels.iloc[position]

        # Transaction ID
        if pd.isna(transaction_id):
            errors[position].append("missing_or_invalid_transaction_id")
        elif transaction_id % 1 != 0:
            errors[position].append("non_integer_transaction_id")
        else:
            transaction_id = int(transaction_id)

            if (
                transaction_id in seen_ids
                or transaction_id in ids_seen_in_this_chunk
            ):
                errors[position].append("duplicate_transaction_id")
            else:
                ids_seen_in_this_chunk.add(transaction_id)

        # Transaction timestamp: relative seconds in this dataset.
        if pd.isna(timestamp):
            errors[position].append("missing_or_invalid_timestamp")
        elif timestamp < 0 or timestamp % 1 != 0:
            errors[position].append("invalid_timestamp")

        # Transaction amount
        if pd.isna(amount):
            errors[position].append("missing_or_invalid_amount")
        elif not (-float("inf") < float(amount) < float("inf")):
            errors[position].append("non_finite_amount")
        elif amount < 0:
            errors[position].append("negative_amount")

        # Fraud label must be 0 or 1.
        if pd.isna(fraud_label) or fraud_label not in (0, 1):
            errors[position].append("invalid_fraud_label")

    # Record all valid integer IDs from this chunk so later chunks
    # can detect repeated IDs, even if another field was invalid.
    for transaction_id in transaction_ids:
        if pd.notna(transaction_id) and transaction_id % 1 == 0:
            seen_ids.add(int(transaction_id))

    error_series = pd.Series(errors, dtype=object)
    rejected_mask = error_series.map(bool).to_numpy()

    accepted = data.iloc[~rejected_mask].copy()
    rejected = data.iloc[rejected_mask].copy()

    rejected["_quality_errors"] = (
    error_series.iloc[rejected_mask]
    .map(lambda items: "|".join(items))
    .to_numpy()
)
    metrics = {
        "input_rows": int(len(data)),
        "accepted_rows": int(len(accepted)),
        "rejected_rows": int(len(rejected)),
        "duplicate_ids_detected": int(
            error_series.map(
                lambda items: "duplicate_transaction_id" in items
            ).sum()
        ),
    }

    return accepted, rejected, metrics
