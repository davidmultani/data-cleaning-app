import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from pipeline.insights import (
    get_column_groups,
    apply_filters,
    aggregate_data,
    get_summary_statistics,
    AGGREGATION_FUNCTIONS,
    PLOT_TYPES
)


def render():
    st.header("Step 3 - Explore Your Data")

    if ("cleaned_df" not in st.session_state or
            st.session_state["cleaned_df"] is None):
        st.warning("⚠️ Please clean your data first.")
        return

    df = st.session_state["cleaned_df"]
    groups = get_column_groups(df)
    # groups = {"numeric":[...], "categorical":[...], "all":[...]}

    # --- SIDEBAR: PLOT CONTROLS ----------------------------------

    with st.sidebar:
        st.header("📊 Plot Controls")
        st.caption("Configure your visualization here.")

        plot_type = st.selectbox("Chart type", options=PLOT_TYPES)

        x_col = st.selectbox(
            "X axis",
            options=groups["all"],
            index=0
            # index=0 selects the first item bu default
        )

        y_options = groups["numeric"] if groups["numeric"] else groups["all"]
        y_col = st.selectbox("Y axis", options=y_options)

        st.divider()

        use_groupby = st.checkbox("Group by a column")
        group_col = None
        agg_func = "mean"

        if use_groupby and groups["categorical"]:
            group_col = st.selectbox(
                "Group by",
                options=groups["categorical"]
            )
            agg_func = st.selectbox(
                "Aggregation",
                options=AGGREGATION_FUNCTIONS
            )

        st.divider()

        color_options = ["None"] + groups["categorical"]
        color_col = st.selectbox("Color by", options=color_options)
        color = None if color_col == "None" else color_col
        # Pass None (not the string "None") to Plotly
        # Plotly ignores color=None but would try to use color="None"
        # as a column name

    # --- FILTERS ---------------------------------------------------
    with st.expander("🔧 Filters", expanded=False):
        # st.expander() creates a collapsible section
        # expanded=False means it starts collapsed

        filters = {}
        st.caption("Filter your data before plotting.")

        # Numeric range sliders
        for col in groups["numeric"][:4]:
            # Limit to first 4 numeric columns to avoid clutter
            col_min = float(df[col].min())
            col_max = float(df[col].max())

            if col_min == col_max:
                continue
            # Skip columns with no range - slider needs min != max

            selected_range = st.slider(
                f"{col}",
                min_value=col_min,
                max_value=col_max,
                value=(col_min, col_max)
                # Passing a tuple creates a range slider with two handles
            )

            if selected_range != (col_min, col_max):
                # Only add to filters if the user actually changed the range
                filters[col] = selected_range

        # Categorical multi-select
        for col in groups["categorical"][:3]:
            # Limit to first 3 categorical columns
            unique_vals = df[col].dropna().unique().tolist()

            if len(unique_vals) <= 1 or len(unique_vals) > 50:
                continue
            # Skip if only one value (no point filtering)
            # or too many values (dropdown would be unusable)

            selected_vals = st.multiselect(
                f"{col}",
                options=unique_vals,
                default=unique_vals
                # st.multiselect() shows a dropdown where multiple items can be selected
                # default = unique_vals means all are selected by default
            )

            if len(selected_vals) < len(unique_vals):
                # Only filter if user deselected something
                filters[col] = selected_vals

    filtered_df = apply_filters(df.copy(), filters)

    filter_info = (
        f"Showing {len(filtered_df):,} of {len(df):,} rows"
        if filters else f"Showing all {len(df):,} rows"
    )
    st.caption(filter_info)

    # --- AGGREGATE IF GROUPBY SELECTED --------------------------

    plot_df = filtered_df
    plot_y = y_col
    plot_x = x_col

    if use_groupby and group_col:
        try:
            plot_df = aggregate_data(filtered_df, group_col, y_col, agg_func)
            plot_y = f"{agg_func}_{y_col}"
            plot_x = group_col
        except Exception as e:
            st.warning(f"Aggregation failed: {e}. Showing raw data.")
            # If aggregation fails (e.g. wrong column types), fall back gracefully

    # --- BUILD AND RENDER CHART ----------------------------------------
    st.subheader(f"{plot_type} Chart")

    try:
        fig = _build_chart(
            plot_type, plot_df, plot_x, plot_y, color, filtered_df, groups
        )

        if fig:
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Could not build chart: {e}")
        st.info("Try selecting different columns or a different chart type.")

    # --- SUMMARY STATISTICS -----------------------------------------

    st.divider()
    st.subheader("Summary Statistics")

    stats = get_summary_statistics(filtered_df)
    if not stats.empty:
        st.dataframe(stats, use_container_width=True)

    else:
        st.info("No numeric columns available for statistics.")


def _build_chart(plot_type, plot_df, x_col, y_col, color, full_df, groups):
    # Builds and Returns a Plotly figure based on the selected chart type.

    # This is a private helper function separated from render()
    # to keep the rendering code clean and focused.
    # Each chart type has its own Plotly Express function.

    if plot_type == "Bar":
        return px.bar(
            plot_df,
            x=x_col,
            y=y_col,
            color=color,
            title=f"{y_col} by {x_col}",
            # title = appears at the top of the chart
            template="plotly_white",
            # template = controls the overall visual style
            # "plotly_white" is clean and professional
        )
    elif plot_type == "Line":
        return px.line(
            plot_df,
            x=x_col,
            y=y_col,
            color=color,
            title=f"{y_col} over {x_col}",
            template="plotly_white",
            markers=True
            # markers=True adds dots at each data point on the line
        )
    elif plot_type == "Scatter":
        return px.scatter(
            plot_df,
            x=x_col,
            y=y_col,
            color=color,
            title=f"{x_col} vs {y_col}",
            template="plotly_white",
            trendline="ols" if not color else None
            # trendline = "ols" adds a linear regression line
            # Only add when not using color (colored scatter + trendline get messy)
            # "ols" = Ordinary Least Squares regression
        )
    elif plot_type == "Histogram":
        return px.histogram(
            plot_df,
            x=x_col,
            color=color,
            title=f"Distribution of {x_col}",
            template="plotly_white",
            nbins=30,
            # nbins = controls how many bars the distribution is divided into
            marginal="box"
            # marginal = "box" adds a small box plot above the histogram
            # showing quartiles and outliers
        )
    elif plot_type == "Box":
        return px.box(
            plot_df,
            x=x_col if x_col in groups["categorical"] else None,
            y=y_col,
            color=color,
            title=f"Distribution of {y_col}",
            template="plotly_white",
            points="outliers"
            # points="outliers shows individual outlier points as dots"
        )
    elif plot_type == "Pie":
        if x_col not in groups["categorical"]:
            return None
        value_counts = (
            plot_df[x_col].value_counts().reset_index()
        )
        value_counts.columns = [x_col, "count"]
        return px.pie(
            value_counts,
            names=x_col,
            values="count",
            title=f"Distribution of {x_col}",
            hole=0.3
            # hole = 0.3 creates a donut chart instead of a solid pie
            # Looks more modern and the center can show total count
        )
    elif plot_type == "Heatmap (Correlation)":
        numeric_df = full_df[groups["numeric"]]
        if len(groups["numeric"]) < 2:
            st.info("Need at least 2 numeric columns for a correlation heatmap.")
            return None

        corr = numeric_df.corr().round(2)
        # .corr() computes the correlation coefficient between every pair
        # of numeric columns, Value range from -1 to 1:
        # 1.0 = perfect positive correlation
        # 0.0 = no linear correlation
        # -1.0 = perfect negative correlation

        return px.imshow(
            corr,
            text_auto=True,
            # text_auto=True prints the correlation value in each cell
            color_continuous_scale="RdBu_r",
            # "RdBu_r" = red (negative) to blue (positive), reversed
            title="Correlation Matrix",
            template="plotly_white",
            zmin=-1,
            zmax=1
            # Fix the color scale range so 0 always maps to white
        )
    return None
