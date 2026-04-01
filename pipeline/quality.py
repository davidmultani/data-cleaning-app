import pandas as pd
import numpy as np
from datetime import datetime


def compute_quality_score(df: pd.DataFrame) -> dict:
    # Computes a data quality score across 4 dimensions and returns
    # a dictionary with scores, breakdown, and a list of issues found.

    # The 4 dimensions are:
    # 1. COMPLETENESS - what fraction of expected data is present?
    # Weight: 40% - the most important dimension. Missing data
    # directly limits what analysis you can do.

    # 2. UNIQUENESS - how much data is duplicated?
    # Weight: 25% - duplicates distort counts, sums, and averages.

    # 3. CONSISTENCY - are numeric values within expected ranges?
    # Weight: 20% - outliers indicate data entry errors or
    # integration problems.

    # 4. VALIDITY - are data types consistent within each column?
    # Weight: 15% - mixed types (some numbers, some strings in the
    # same column) suggest data was combined incorrectly.

    results = []
    issues = []
    # issues is a list of human-readable problem descriptions
    # shown to the user as warnings in the UI

    # -- DIMENSION 1: COMPLETENESS -------------------------------

    total_cells = df.shape[0] * df.shape[1]
    # Total number of individual data cells in the entire DataFrame
    # shape[0] = rows, shape[1] = columns, multiple for total cells

    missing_cells = int(df.isna().sum().sum())
    # df.isna() -> boolean DataFrame
    # .sum() -> sum each column -> Series of missing counts per column
    # .sum() -> again -> sum all those counts -> total missing cells
    # int() converts numpy int to plain Python int

    completeness_score = round(
        100 * (1 - missing_cells / total_cells), 1
    ) if total_cells > 0 else 100.0

    # Per-column completeness breakdown
    col_completeness = {}
    for col in df.columns:
        pct = round(100 * df[col].notna().mean(), 1)
        # .notna().mean() = fraction of not-null values = completeness fraction.

        col_completeness[col] = pct

        if pct < 70:
            issues.append(
                f"Column '{col} is only {pct}% complete'"
                f"({df[col].isna().sum()} missing values)"
            )

    results["completeness"] = {
        "score": completeness_score,
        "per_column": col_completeness,
        "description": "Percentage of non-null values across all cells",
        "weight": 0.40
    }

    # --- DIMENSION 2: UNIQUENESS -------------------------------------

    total_rows = len(df)
    duplicate_rows = int(df.duplicated().sum())
    # .duplicated() returns True for every row that is a duplicate
    # of an earlier row (the first occurrence is False)

    uniqueness_score = round(
        100 * (1 - duplicate_rows / total_rows), 1
    ) if total_rows > 0 else 100.0

    if duplicate_rows > 0:
        issues.append(
            f"{duplicate_rows} duplicate row(s) detected"
            f"({round(100 * duplicate_rows / total_rows, 1)}% of all rows)"
        )

    # Check ID-like columns - they should be unique by definition.
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ["id", "key", "uuid", "code"]):
            # any() returns True if at least one keyword is found in the column name

            dup_count = int(df[col].duplicated().sum())
            if dup_count > 0:
                issues.append(
                    f"Column '{col}' looks like an ID but has"
                    f"{dup_count} duplicate value(s)"
                )

    results["uniqueness"] = {
        "score": uniqueness_score,
        "duplicated_rows": duplicate_rows,
        "description": "Percentage of rows that are not duplicates",
        "weight": 0.25
    }

    # --- DIMENSION 3: CONSISTENCY ---------------------------------

    consistency_scores = []

    for col in df.select_dtypes(include=[np.number]).columns:
        series = df[col].dropna()
        if len(series) < 4:
            consistency_scores.append(100.0)
            continue
            # Cannot computer IQR with too few values - assume perfect

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0:
            consistency_scores.append(100.0)
            continue
            # IQR of 0 means all values are the same - no outliers possible

        n_outliers = int(((series < q1 - 1.5 * iqr) |
                          (series > q3 + 1.5 * iqr)).sum())

        outliers_pct = round(100 * n_outliers / len(series), 1)

        # Score decreases proportionally with outlier percentage
        col_score = max(0.0, 100.0 - outliers_pct * 10)
        # Each 1% of outliers reduce the score by 10 points
        # max(0.0, ...) prevents negative score
        consistency_scores.append(col_score)

        if n_outliers > 0:
            issues.append(
                f"Column '{col}' has {n_outliers} statistical outlier(s)"
                f"({outliers_pct}% of values)"
            )

    consistency_score = round(
        sum((consistency_scores) / len(consistency_scores), 1)
        if consistency_scores else 100.0
    )
    # Average the per-column score
    # If no numeric columns, default to 100

    results["consistency"] = {
        "score": consistency_score,
        "description": "Numeric columns free from statistical outliers",
        "weight": 0.20
    }

    # --- DIMENSION 4: VALIDITY -----------------------------------
    validity_scores = []

    for col in df.select_dtypes(include="object").columns:
        # Only check object columns - numeric and datetime are already typed

        series = df[col].dropna()
        if len(series) == 0:
            validity_scores.append(100.0)
            continue

        # Check if the column has mixed Python types
        # (some values are int, some are str - indicates bad data)
        type_series = series.apply(type)
        # .apply(type) applies Python's built-in type() to every value
        # Returns a Series of type objects like [str, str, int, str]

        unique_types = type_series.nunique()

        if unique_types > 1:
            # More than one Python type in the column - inconsistent
            dominant_count = type_series.value_counts().iloc[0]
            # .value_counts() counts occurrences of each type
            # .iloc[0] gets the count of the most common type

            dominant_pct = round(100 * dominant_count / len(series), 1)
            validity_scores.append(dominant_pct)

            issues.append(
                f"Column '{col}' contains mixed data types"
                f"(dominant type is {dominant_pct}% of values)"
            )
        else:
            validity_scores.append(100.0)

    validity_score = (
        round(sum(validity_scores) / len(validity_scores), 1)
        if validity_scores else 100.0
    )

    results["validity"] = {
        "score": validity_score,
        "description": "Values conform to consistent data types",
        "weight": 0.15
    }

    # -- OVERALL WEIGHTED SCORE --------------------------------
    overall = round(
        results["completeness"]["score"] * 0.40 +
        results["consistency"]["score"] * 0.25 +
        results["uniqueness"]["score"] * 0.20 +
        results["validity"]["score"] * 0.15,
        1
    )

    grade = (
        "A" if overall >= 90 else
        "B" if overall >= 75 else
        "C" if overall >= 60 else
        "D"
    )
    # Ternary chain - evaluates conditions in order, returns first True match

    results["overall"] = overall
    results["grade"] = grade
    results["issues"] = issues
    results["issue_count"] = len(issues)
    results["computed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return results
