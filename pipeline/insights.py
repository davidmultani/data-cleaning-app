import pandas as pd
import numpy as np

# These are aggregation function names that pandas .agg() understands
# Defined at module level so UI can import this list to build dropdowns
AGGREGATION_FUNCTIONS = ["mean", "sum", "count", "min", "max", "median"]

PLOT_TYPES = ["Bar", "Line", "Scatter", "Histogram",
              "Box", "Pie", "Heatmap (Correlation)"]


def get_column_groups(df: pd.DataFrame) -> dict:
    # Categorizes columns by their data type.
    # Returns a dictionary with three lists:
    #   "numeric" - int and float columns
    #   "categorical" - object/string columns
    #   "datetime" - datetime columns
    #   "all" - all column names

    # Used by the UI to populate the right columns into each dropdown.
    # For example: X-axis can be anything, Y-axis should be numeric.

    return {
        "numeric": df.select_dtypes(include=[np.number]).columns.tolist(),
        "categorical": df.select_dtypes(include="object").columns.tolist(),
        "datetime": df.select_dtypes(include="datetime").columns.tolist(),
        "all": df.columns.tolist()
    }


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    # Filters the DataFrame based on user-selected conditions.

    # fiters is a dictionary where:
    #   key = column_name
    #   value = either:
    #       - a tuple (min, max) for numeric range filters
    #       - a list of values for categorical selection filters

    # Example:
    #   filters = {
    #        "age": (25,45),
    #        "department": ["Engineering","HR"]
    #   }

    for col, condition in filters.items():

        if col not in df.columns:
            continue

        if isinstance(condition, tuple) and len(condition) == 2:
            lo, hi = condition
            # Unpack the tuple into low and high values
            df = df[df[col].between(lo, hi)]
            # .between(lo, hi) returns True for rows within the range (inclusive)
            # df[boolean_series] keeps only the True rows

        elif isinstance(condition, list) and len(condition) > 0:
            df = df[df[col].isna(condition)]
            # isna(list) returns True for rows whose value is in the list

    return df


def aggregate_data(df: pd.DataFrame,
                   group_col: str,
                   value_col: str,
                   agg_func: str) -> pd.DataFrame:

    # Groups the DataFrame by group_col and aggregates value_col.

    # Example:
    #       group_col = "department"
    #       value_col = "salary"
    #       agg_func = "mean"
    # Result: one row per department with the average salary

    # The result column is renamed to "mean_salary" so the
    # chart axis label is descriptive instead of just "salary".

    result = (
        df.groupby(group_col)[value_col]
        # .groupny() splits the DataFrame into one group per unique value
        # [value_col] selects only the column to aggregate within each group

        .agg(agg_func)
        # .agg("mean") computes the mean within each group
        # pandas looks up "mean" as a built-in aggregation

        .reset_index()
        # After groupby, the group column becomes the index
        # reset_index() brigs it back as a regular column

        .rename(columns={value_col: f"{agg_func}_{value_col}"})
        # Rename "salary" -> "mean_salary" for clear axis label

        .sort_values(f"{agg_func}_{value_col}", ascending=False)
        # Sort by the aggregated value descending
        # Makes bar chart show largest bars first - easier to read

        .reset_index(drop=True)
        # Reset index again after sorting
    )
    return result


def get_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    # Return descriptive statistics for all numeric columns.
    # This is a thin Wrapper around pandas .describe() that
    # rounds the output and adds a "column" label.

    numeric_df = df.select_dtypes(include=[np.number])

    if numeric_df.empty:
        return pd.DataFrame()

    stats = numeric_df.describe().round(2)
    # .describe() computes: count, mean, std, min, 25%, 50%, 75%, max
    # .round(2) rounds all values to 2 decimal places

    return stats.T
    # .T transposes the DataFrame - swaps rows and columns
    # This puts column names as rows and statistics as columns
    # Which reads more naturally as a table in the UI
