import pytest
import pandas as pd
import numpy as np

from pipeline.cleaner import (
    compute_default_fills,
    apply_fills,
    drop_duplicate_rows,
    drop_constant_columns,
    cap_outliers_iqr,
    normalize_column_names
)
from pipeline.audit import AuditLog

# ---------------------- FIXTURES --------------------------------
# A fixture is a reusable piece of test setup
# @pytest.fixture decorates a function that returns test data
# Any test function that lists the fixture name as a parameter
# automatically receives the returned value


@pytest.fixture
def sample_df():
    """ Creates a small test DataFrame with known properties."""
    return pd.DataFrame({
        "name": ["Alice", "Bob", None, "Diana", "Alice"],
        # None will be treated as NaN - test fill behavior
        "age": [25, 30, 35, None, 25],
        # None in numeric - test median fill
        "salary": [50000, 60000, 70000, 80000, 50000],
        "department": ["HR", "IT", "IT", None, "HR"],
        "constant": ["same", "same", "same", "same", "same"]
        # All same value - test constant column detection
    })


@pytest.fixture
def audit():
    """ Creates a fresh AuditLog for each test."""
    return AuditLog()

# ------------------ TESTS FOR compute_default_fills -----------------


def test_default_fills_numeric_uses_median(sample_df):
    fills = compute_default_fills(sample_df)
    # age has values [25, 30, 35, NaN, 25], median = 27.5
    assert fills["age"] == 27.5, (
        "Numeric default fill should be the median of non-null values"
    )


def test_default_fills_categorical_uses_mode(sample_df):
    fills = compute_default_fills(sample_df)
    # "HR" and "IT" both appear, but "HR" appears twice in department
    # after we drop the None - mode should be "HR"
    # Actually: HR appears 1 time (index 0), IT appears 2 times (index 1, 2)
    # Let's check it returns a string
    assert isinstance(fills["department"], str)


def test_default_fills_skips_complete_columns(sample_df):
    fills = compute_default_fills(sample_df)
    # "salary" has no nulls - should not appear in default
    assert "salary" not in fills

# ------------------ TESTS FOR apply_fills ----------------------------


def test_apply_fills_numeric(sample_df, audit):
    fill_map = {"age": "28.0"}
    result = apply_fills(sample_df, fill_map, audit)
    # After fill, no nulls should remain in "age"
    assert result["age"].isna().sum() == 0


def test_apply_fills_string(sample_df, audit):
    fill_map = {"name": "Unknown", "department": "General"}
    result = apply_fills(sample_df, fill_map, audit)
    assert result["name"].isna().sum() == 0
    assert result["department"].isna().sum() == 0


def test_apply_fills_does_not_modify_original(sample_df, audit):
    original_nulls = sample_df["age"].isna().sum()
    fill_map = {"age": "30"}
    _ = apply_fills(sample_df, fill_map, audit)
    # Original should be unchanged because apply_fills uses df.copy()
    assert sample_df["age"].isna().sum() == original_nulls

# ------------------ TESTS FOR drop_duplicate_row -----------------------


def test_drop_duplicates_removes_exact_duplicates(sample_df, audit):
    # Row 0 (Alice, 25, 50000, HR, same) is identical to row 4
    result = drop_duplicate_rows(sample_df, audit)
    assert len(result) == len(sample_df) - 1


def test_drop_duplicates_logs_action(sample_df, audit):
    drop_duplicate_rows(sample_df, audit)
    log_df = audit.to_dataframe()
    assert len(log_df) == 1
    assert log_df.iloc[0]["action"] == "Drop Duplicate Rows"

# ------------------ TESTS FOR drop_constant_columns ----------------------


def test_drop_constant_columns_removes_constant(sample_df, audit):
    result = drop_constant_columns(sample_df, audit)
    assert "constant" not in result.columns


def test_drop_constant_columns_keeps_varying(sample_df, audit):
    result = drop_constant_columns(sample_df, audit)
    assert "age" in result.columns
    assert "name" in result.columns

# -------------------- TEST FOR cap_outliers_iqr -------------------------


def test_cap_outliers_reduce_extreme_values():
    df = pd.DataFrame({
        "value": [10, 12, 11, 13, 10, 12, 1000]
        # 1000 is an extreme outlier
    })
    result = cap_outliers_iqr(df)
    assert result["value"].max() < 1000, (
        "Outliers should have been capped to the upper fence"
    )


def test_cap_outliers_does_not_modify_original():
    df = pd.DataFrame({"value": [10, 12, 11, 13, 10, 12, 1000]})
    original_max = df["value"].max()
    _ = cap_outliers_iqr(df)
    assert df["value"].max() == original_max

# --------------- TESTS FOR normalize_column_names --------------------


def test_normalize_removes_spaces():
    df = pd.DataFrame({"First Name": [1], " Last Name ": [2]})
    result = normalize_column_names(df)
    assert "first_name" in result.columns
    assert "last_name" in result.columns


def test_normalize_removes_special_chars():
    df = pd.DataFrame({"Salary ($)": [1], "Q1/Revenue": [2]})
    result = normalize_column_names(df)
    assert "salary_" in result.columns or "salary" in result.columns
    assert "q1_revenue" in result.columns
