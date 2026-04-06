import pytest
import pandas as pd
import numpy as np

from pipeline.quality import compute_quality_score


@pytest.fixture
def perfect_df():
    """A clean DataFrame with no issues. """
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "salary": [50000, 60000, 70000, 55000, 65000]
    })


@pytest.fixture
def messy_df():
    """A DataFrame with several quality issues. """
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 1],
        # Duplicate ID (1 appears twice)
        "name": ["Alice", None, None, "Diana", "Alice"],
        # 2 missing values
        "age": [25, 30, 35, None, 25],
        # 1 missing value
        "salary": [50000, 60000, 70000, 55000, 50000]
    })


def test_perfect_df_scores_high(perfect_df):
    result = compute_quality_score(perfect_df)
    assert result["overall"] >= 90, (
        "A clean DataFrame should score at least 90%"
    )


def test_perfect_df_grade_is_a(perfect_df):
    result = compute_quality_score(perfect_df)
    assert result["grade"] == "A"


def test_messy_df_score_lower(messy_df):
    result = compute_quality_score(messy_df)
    assert result["overall"] < 90, (
        "A DataFrame with nulls and duplicates should score below 90%"
    )


def test_completeness_reflects_missing_values(messy_df):
    result = compute_quality_score(messy_df)
    # We have 3 missing values in 5 rows x 4 cols = 20 cells
    # Completeness = (20-3)/20 = 85%
    assert result["completeness"]["score"] < 95


def test_uniqueness_reflects_duplicates(messy_df):
    result = compute_quality_score(messy_df)
    # Row 0 (Alice) and row 4 (Alce) are duplicates - 1 out of 5
    # uniqueness = 4/5 = 80%
    assert result["uniqueness"]["score"] < 90


def test_result_has_all_required_keys(perfect_df):
    result = compute_quality_score(perfect_df)
    required_keys = [
        "overall", "grade", "issues", "issue_count",
        "completeness", "uniqueness", "consistency", "validity"
    ]
    for key in required_keys:
        assert key in result, f"Missing key: {key}"
