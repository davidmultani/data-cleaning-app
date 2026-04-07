import pandas as pd
import numpy as np
# numpy is needed for np.number which selects all numeric dtypes at once
# and for mathematical operations on arrays

from pipeline.audit import AuditLog
# Import AuditLog so each function can record what it did


def compute_default_fills(df: pd.DataFrame) -> dict:
    # Computes a sensible default fill value for every column that has nulls.

    # Returns a dictionary like:
    #   {"age":30,"salary":65000.0,"department":"Engineering"}

    # This is shown to user BEFORE they apply any cleaning,
    # so they can see what the defaults are and decide whether to override.

    # Why these defaults:
    # - Numeric -> median (not mean) because median is not affected by outliers.
    # A salary column with one $10M entry would pull the mean far from
    # typical value. Median stays representative regardless.

    # - Datetime -> "ffill" (forward fill) because dates are sequential.
    # A missing date is most likely close to the surrounding dates.

    # - Categorical -> mode (most common value) because the most frequent
    # category is the best guess when no other information is available.

    default = {}
    # Empty dictionary, we will add one entry per column that has nulls

    for col in df.columns:
        if df[col].isna().sum() == 0:
            continue
        # continue skips the rest of the loop body for this column
        # and jumps to the next iteration
        # no nulls = no fill needed = skip it

        if pd.api.types.is_numeric_dtype(df[col]):
            # is_numeric_dtypes() returns True for int64, float64, etc.
            default[col] = round(float(df[col].median()), 4)
            # .median() finds the middle value when sorted
            # float() converts numpy types to plain Python float
            # round(...., 4) limits decimal places to 4

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            # is_datetime64_any_dtype() catches all datetime variants
            default[col] = "ffill"
            # Store the string "ffill" as a signal
            # The apply_fills function knows to use forward-fill
            # when it sees this value

        else:
            # Everything else: strings, categories, mixed types
            mode_series = df[col].mode()
            # .mode() returns a series of most frequent values
            # It returns a Series not a scaler because there can be ties

            if len(mode_series) > 0:
                default[col] = str(mode_series.iloc[0])
                # .iloc[0] gets the first (highest frequency) value
                # str() ensures it is a plain string

            else:
                default[col] = "Unknown"
                # Fallback for completely empty columns

    return default


def apply_fills(df: pd.DataFrame, fill_map: dict, audit_log: AuditLog = None) -> pd.DataFrame:
    # Fills missing values using the provided fill_map dictionary.

    # fill_map comes from the UI - it starts as the computed defaults
    # but the user may have changed any values.

    # Example fill map:
    #   {"age":"31.0", "salary":"65000.0", "department":"Engineering"}

    # ** Note that all values come from text input boxes as strings,
    # so we cast them to the appropriate type before filling.

    df = df.copy()
    # ALWAYS copy before modifying
    # Without this, you modify the original DataFrame stored in session_state
    # which causes confusing bugs where "undo" is impossible

    for col, value in fill_map.items():
        # .items() returns (key, value) pairs from the dictionary
        # col = column name, value = the fill value as a string

        if col not in df.columns:
            continue
        # Safety check - the column might have been dropped already

        n_missing = df[col].isna().sum()
        # Count nulls BEFORE filling, for the audit log

        if n_missing == 0:
            continue
        # No nulls in this column anymore - skip it

        if value == "ffill":
            # Special case: forward-fill for datetime columns
            df[col] = df[col].ffill()
            # .ffill() propagates the last valid value forward
            df[col] = df[col].bfill()
            # .bfill() propagates the next valid value backward
            # We chain bfill and after ffill to handle NaN at the very start
            # (ffill can't fill first row because their is nothing before it)

            if audit_log:
                audit_log.log(action="Fill Null Values",
                              detail=(f"Column '{col}' : filled {n_missing} missing "
                                      f"datetime value(s) using forward/backward fill")
                              )

        elif pd.api.types.is_numeric_dtype(df[col]):
            try:
                fill_val = float(value)
            # Try to convert the string to a float
            # float() works for both integers and decimals
            except ValueError:
                fill_val = df[col].median()
            # If the user typed something non-numeric, fall back to median
            # This prevents the app from crashing on bad user input.

            df[col] = df[col].fillna(fill_val)
            # .fillna(value) replaces all NaN entries with the given value

            if audit_log:
                audit_log.log(action="Fill Null Values",
                              detail=(f"Column '{col}': filled {n_missing} missing "
                                      f"value(s) with {fill_val}")
                              )

        else:
            # String / categorical column
            fill_val = str(value).strip()
            # str() ensures it is a string
            # .strip() removes any accidental leading/trailing sapces

            df[col] = df[col].fillna(fill_val)

            if audit_log:
                audit_log.log(action="Fill Null Values",
                              detail=(f"Column '{col}': filled {n_missing} missing "
                                      f"value(s) with '{fill_val}'")
                              )

    return df


def normalize_column_names(df: pd.DataFrame,
                           audit_log: AuditLog = None) -> pd.DataFrame:
    # Cleans up column names to be consistent and code-friendly.
    # Example:
    #       "  First Name  " -> "first_name"
    #       "Join-Date" -> "join_date"
    #       "Salary ($)" -> "salary_"
    #       "Q1/Revenue" -> "q1_revenue"

    # Why this matters: inconsistent column names cause bugs when
    # you reference them in code. A space in a column name means
    # you cannnot write df.first_name - you must write df["first name"].
    # Normalized names are also easier to read and type.

    df = df.copy()
    old_names = df.columns.tolist()
    # Save original names for the audit log

    df.columns = (
        df.columns.str.strip()
        # Removes leading and trailing whitespace from all column names
        # "   Name   " -> "Name"

        .str.lower()
        # Convert to lowercase
        # "FirstName" -> "firstname"

        .str.replace(r"[\s\-\/\.\,]+", "_", regex=True)
        # Replace one or more of: spaces, hyphens, slashes, dots, commas
        # with a single underscore
        # "first name" -> "first_name"
        # "join-date" -> "join_date"
        # regex=True tells pandas this is a regex pattern, not a literal string

        .str.replace(r"[^\w]", "", regex=True)
        # Remove remaining characters that are not word characters
        # \w matches letters, digits, and underscore
        # [^\w] matches anything that is NOT a word character
        # "salary($)" -> "salary"

        .str.replace(r"_+", "_", regex=True)
        # Replaces multiple consecutive underscores with a single one
        # "first__name" -> "first_name"

        .str.strip("_")

    )
    new_names = df.columns.to_list()
    renamed = {o: n for o, n in zip(old_names, new_names) if o != n}
    # Dictionary comprehension: creates a dict of only the columns that changed
    # zip(old_names, new_names) pairs them up: [("Name", "name"),.....]

    if audit_log and renamed:
        audit_log.log(
            action="Normalize Column Names",
            detail=(f"Renamed {len(renamed)} column(s): "
                    + ", ".join(f"'{o}' -> '{n}'" for o, n in renamed.items()))
        )

    return df


def drop_high_missing_columns(df: pd.DataFrame,
                              threshold: float = 0.5,
                              audit_log: AuditLog = None) -> pd.DataFrame:
    # Drops columns where more than 'threshold' fraction of values are missing.

    # Default threshold is 0.5 meaning columns with >50% missing are dropped.

    # Why: A column that is 80% empty provides very little information.
    # keeping it adds noise and can mislead analysis. It is better to
    # drop it and note that it was dropped in the audit log.

    cols_before = df.shape[1]
    # df.shape returns (rows, column) - [1] gets columns

    missing_fraction = df.isna().mean()
    # df.isna() returns a boolean DataFrame (True Where NaN)
    # .mean() on a boolean column = fraction of True values
    # Result is a Series like: {"age":0.0, "notes": 0.73, "id": 0.0}

    cols_to_drop = missing_fraction[missing_fraction >
                                    threshold].index.tolist()
    # Filter to only columns exceeding the threshold
    # .index gives the column names
    # .to_list() converts to a plain Python list

    df = df.drop(columns=cols_to_drop)
    # .drop(columns=list) removes those columns from the DataFrame

    cols_after = df.shape[1]

    if audit_log and cols_to_drop:
        audit_log.log(
            action="Drop High-Missing Columns",
            detail=(f"Dropped {len(cols_to_drop)} column(s) with "
                    f"> {threshold*100:.0f}% missing: {cols_to_drop}"),
            cols_before=cols_before,
            cols_after=cols_after
        )
    return df


def drop_duplicate_rows(df: pd.DataFrame,
                        audit_log: AuditLog = None) -> pd.DataFrame:

    # Removes rows where every column value is identical to another row.

    # Why: Duplicate rows distort aggregations (average, totals become wrong),
    # inflate row counts, and usually represent data entry errors or
    # import mistakes rather than real repeated observations.

    rows_before = len(df)

    df = df.drop_duplicates()
    # Compares every row to every other row
    # Keeps the first occurrence of each duplicate, removes the rest

    df = df.reset_index(drop=True)
    # After dropping rows, the index has gaps like 0, 1, 3, 5....
    # reset_index() renumbers from 0 cleanly
    # drop=True discards the old index instead of saving it as a column

    rows_after = len(df)
    removed = rows_before - rows_after

    if audit_log:
        audit_log.log(
            action="Drop Duplicate Rows",
            detail=f"Removed {removed} duplicate row(s)",
            rows_before=rows_before,
            rows_after=rows_after
        )

    return df


def drop_fully_empty_rows(df: pd.DataFrame,
                          audit_log: AuditLog = None) -> pd.DataFrame:
    # Removes rows where every single column is NaN.

    # These rows carry zero information and are usually caused by
    # extra blank lines at the bottom of the spreadsheet.

    rows_before = len(df)

    df = df.dropna(how="all")
    # how="all" means: only drop a row if ALL values are NaN
    # (contrast with how="any" which drops if ANY value is NaN - too aggressive)

    df = df.reset_index(drop=True)
    rows_after = len(df)

    if audit_log:
        audit_log.log(
            action="Drop Empty Rows",
            detail=f"Removed {rows_before - rows_after} fully empty row(s)",
            rows_before=rows_before,
            rows_after=rows_after
        )
    return df


def drop_constant_columns(df: pd.DataFrame,
                          audit_log: AuditLog = None) -> pd.DataFrame:
    # Removes columns where every value is the same.

    # Why: A column with the same value is every row (e.g., "country": "India")
    # for every row) adds no analytical value. Every group, filter, or
    # correlation involving it will produce the same result.

    cols_before = df.shape[1]

    constant_cols = [
        col for col in df.columns
        if df[col].nunique(dropna=False) <= 1
        # nunique(dropna=False) counts distinct values including NaN
        # <= 1 catches: all same value, or all NaN, or single value + NaN
    ]

    df = df.drop(columns=constant_cols)
    cols_after = df.shape[1]

    if audit_log:
        audit_log.log(
            action="Drop Constant Columns",
            detail=(f"Dropped {len(constant_cols)} constant column(s): "
                    f"{constant_cols}"),
            cols_before=cols_before,
            cols_after=cols_after
        )
    return df


def cap_outliers_iqr(df: pd.DataFrame,
                     multiplier: float = 1.5,
                     audit_log: AuditLog = None) -> pd.DataFrame:

    # Caps extreme outliers in numeric columns using the IQR method.

    # IQR (Interquartile Range) - Q3 - Q1
    # Lower fence = Q1 - (multiplier * IQR)
    # Upper fence = Q3 = (multiplier * IQR)

    # Values below the lower fence are set TO the lower fence value.
    # Values above the upper fence are set TO the upper fence value.

    # This is called "winsorizing" - you are not deleting outliers,
    # Z-score assumes a normal (bell-curve) distribution.
    # Real-world data is rarely normal. IQR works on any distribution
    # and is not itself influenced by the outliers it is detecting.

    # Why cap instead of delete:
    # Deleting rows loses information. Capping preserves the row but
    # reduces the influence of extreme values on analysis.

    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    # select_dtypes(include=[np.number]) filters to int and float columns
    # np.number matches both - you do not need to specify each separately

    for col in numeric_cols:
        series = df[col].dropna()
        # Works only on non-null values for percentile calculation

        if len(series) < 4:
            continue
            # Cannot compute meaningful quartiles with fewer than 4 values

        q1 = series.quantile(0.25)
        # 25th percentile - 25% of values are below this

        q3 = series.quantile(0.75)
        # 75th percentile - 75% of values are below this

        iqr = q3 - q1
        # The spread of the middle 50% of data

        lower_fence = q1 - (multiplier * iqr)
        upper_fence = q3 + (multiplier * iqr)
        # Standard IQR fences - values outside these are statistical outliers

        n_outliers = ((df[col] < lower_fence) | (df[col] > upper_fence)).sum()
        # Count values outside both fences
        # | is the "or" operator for boolean arrays

        if n_outliers > 0:
            df[col] = df[col].clip(lower=lower_fence, upper=upper_fence)
            # .clip() sets any value below lower to lower,
            # and any value above upper to upper
            # Values in between are unchanged

            if audit_log:
                audit_log.log(
                    action="Cap Outliers",
                    detail=(f"Column '{col}': capped {n_outliers} outlier(s) "
                            f"to [{lower_fence:.2f}, {upper_fence:.2f}]")
                )
    return df


def infer_and_convert_dtypes(df: pd.DataFrame,
                             audit_log: AuditLog = None) -> pd.DataFrame:
    # Attempts to convert object (string) columns to numeric or datetime.

    # Why: pandas reads every ambiguous column as "object" (string) type.
    # A column containing "29", "31", "28" is read as string.
    # Converting to numeric unlocks math operations, statistics, and charts.

    # We attempt numeric conversion first, then datetime.
    # If neither works, we leave the column as-is.

    df = df.copy()

    for col in df.select_dtypes(include="object").columns:
        # Only attempt conversion on object (string) columns
        # Numeric and datetime columns are already correct

        # -- Try numeric conversion --
        converted = pd.to_numeric(df[col], errors="coerce")
        # errors="coerce" converts non-numeric values to NaN instead of crashing
        # If "abc" is in the column, it becomes NaN

        success_rate = converted.notna().mean()

        if success_rate >= 0.8 and converted.notna().sum() > 0:
            # If 80% or more converted successfully, accepts the conversion
            # The 80% threshold allows for some messy data in otherwise numeric columns
            df[col] = converted

            if audit_log:
                audit_log.log(
                    action="Convert Column Type",
                    detail=f"Column '{col}': converted to numeric"
                )
            continue
        # Move to next column - no need to try datetime

        # --- Try datetime conversion ---
        try:
            converted_dt = pd.to_datetime(
                df[col], format="mixed", errors="coerce")
            success_rate_dt = converted_dt.notna().mean()

            if success_rate_dt >= 0.8:
                df[col] = converted_dt

                if audit_log:
                    audit_log.log(
                        action="Convert Column Type",
                        detail=f"Column '{col}': converted to datetime"
                    )
        except Exception:
            pass
            # pass means "do nothing and continue"
            # If datetime conversion completely fails, leave the column alone

    return df
