
import pandas as pd

from src.quality_rules import inspect_schema, validate_transactions


def make_transactions():
    return pd.DataFrame({
        "TransactionID": [1, 2, 2, None],
        "TransactionDT": [10, 20, 30, 40],
        "TransactionAmt": [10.0, -1.0, 5.0, 2.0],
        "ProductCD": ["W", "W", "C", "R"],
        "isFraud": [0, 1, 0, 1],
    })


def test_valid_row_is_accepted():
    df = make_transactions().iloc[[0]].copy()

    accepted, rejected, metrics = validate_transactions(df)

    assert len(accepted) == 1
    assert len(rejected) == 0
    assert metrics["accepted_rows"] == 1


def test_invalid_rows_are_rejected():
    df = make_transactions()

    accepted, rejected, metrics = validate_transactions(df)

    assert len(accepted) == 1
    assert len(rejected) == 3
    assert metrics["input_rows"] == 4


def test_negative_amount_has_error_reason():
    df = make_transactions().iloc[[1]].copy()

    accepted, rejected, _ = validate_transactions(df)

    assert len(accepted) == 0
    assert "negative_amount" in rejected.iloc[0]["_quality_errors"]


def test_duplicate_transaction_id_is_rejected():
    df = make_transactions().iloc[[0, 0]].copy()

    accepted, rejected, metrics = validate_transactions(df)

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert metrics["duplicate_ids_detected"] == 1


def test_duplicates_across_chunks_are_detected():
    seen_ids = set()

    first_chunk = make_transactions().iloc[[0]].copy()
    second_chunk = make_transactions().iloc[[0]].copy()

    accepted_first, rejected_first, _ = validate_transactions(
        first_chunk, seen_ids
    )
    accepted_second, rejected_second, _ = validate_transactions(
        second_chunk, seen_ids
    )

    assert len(accepted_first) == 1
    assert len(rejected_first) == 0
    assert len(accepted_second) == 0
    assert len(rejected_second) == 1


def test_missing_required_column_raises_error():
    df = make_transactions().drop(columns=["isFraud"])

    try:
        validate_transactions(df)
        assert False, "Expected ValueError for missing required column"
    except ValueError as exc:
        assert "isFraud" in str(exc)


def test_schema_baseline_creation_and_drift(tmp_path):
    baseline = tmp_path / "schema_baseline.json"

    original_columns = [
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "ProductCD",
        "isFraud",
    ]

    first = inspect_schema(original_columns, baseline)
    assert first["status"] == "BASELINE_CREATED"

    unchanged = inspect_schema(original_columns, baseline)
    assert unchanged["status"] == "PASS"

    changed_columns = original_columns + ["NewColumn"]
    changed = inspect_schema(changed_columns, baseline)

    assert changed["status"] == "DRIFT_DETECTED"
    assert changed["unexpected_columns"] == ["NewColumn"]


def test_missing_required_schema_column_fails(tmp_path):
    baseline = tmp_path / "schema_baseline.json"

    original_columns = [
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "ProductCD",
        "isFraud",
    ]

    inspect_schema(original_columns, baseline)

    changed_columns = [
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "ProductCD",
    ]

    result = inspect_schema(changed_columns, baseline)

    assert result["status"] == "FAIL"
    assert "isFraud" in result["missing_required_columns"]
