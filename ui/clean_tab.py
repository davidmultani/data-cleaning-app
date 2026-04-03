import streamlit as st
import plotly.graph_objects as go

from pipeline.cleaner import (
    compute_default_fills,
    apply_fills,
    normalize_column_names,
    drop_high_missing_columns,
    drop_duplicate_rows,
    drop_fully_empty_rows,
    drop_constant_columns,
    cap_outliers_iqr,
    infer_and_convert_dtypes
)
from pipeline.quality import compute_quality_score
from pipeline.audit import AuditLog


def render():
    st.header("Step 2 - Clean Your Data")

    # ---- GUARD: require data to be loaded first ---------------------

    if "df" not in st.session_state or st.session_state["df"] is None:
        st.warning("Please upload a file or connect to a database first.")
        return
        # return exists the render() function immediately
        # Nothing below this line runs if there is no data

    df = st.session_state["df"]
    audit_log = st.session_state.get("audit_log", AuditLog())
    # .get() retrieves a value from session_state safely
    # Second argument is the default if the key doesn't exist

    # -- SECTION 1: DEFAULT FILL VALUES -------------------------

    st.subheader("1 - Default Fill Value")
    st.write(
        "These values will be used to fill missing data. "
        "They are computed automatically from your dataset. "
        "You can override any of them below. "
    )

    defaults = compute_default_fills(df)
    # Compute before showing - the user needs to see these before deciding
    # whether to change them

    if defaults:
        # Only show this section if these are any columns with nulls

        defaults_display = {
            col: (f"forward/backward fill (datetime)"
                  if val == "ffill" else str(val))
            for col, val in defaults.items()
        }
        # Replace "ffill" with a more human-readable explanation
        # Dictionary comprehension: {key: new_value for key, val in dict.items()}

        st.table(defaults_display)
        # st.table() renders a clean static table
        # Good for small data like this default summary

    else:
        st.success("No missing values found - your dataset is complete!")

    # -- SECTION 2: OVERRIDE FILL VALUES --------------------------------

    cols_with_nulls = [
        col for col in df.columns if df[col].isna().any()
    ]

    if cols_with_nulls:

        st.subheader("2 - Override Fill Values (Optional)")
        st.write(
            "Each input below is pre-filled with the computed default. "
            "Leave as-is to accept the default. or type a new value. "
        )

        user_fills = {}
        # Dictionary to collect the user's choosen fill values

        for col in cols_with_nulls:
            missing_count = df[col].isna().sum()
            missing_pct = round(100 * missing_count/len(df), 1)
            default_val = str(defaults.get(col, ""))

            user_input = st.text_input(
                label=(
                    f"**{col}** "
                    f"[{df[col].dtype}] - "
                    f"{missing_count} missing ({missing_pct}%)"
                ),
                value=default_val,
                key=f"fill_input_{col}"
                # Every widget needs a unique key in Streamlit
                # Using the column name ensures uniqueness here
            )

            user_fills[col] = user_input.strip(
            ) if user_input.strip() else default_val
            # Use user's input if they typed something
            # Falls back to default if they cleared the box
            # .strip() removes accidental whitespaces

    # --- SECTION 3: TRANSFORMATION OPTIONS -------------------------------------------

    st.subheader("3 - Transformation Options")

    col_a, col_b = st.columns(2)
    # Two-column layout for the checkboxes

    with col_a:
        do_infer_types = st.checkbox("Auto-detect column types", value=True)
        do_normalize = st.checkbox("Normalize column names", value=True)
        do_drop_empty = st.checkbox("Drop fully empty rows", value=True)
        do_drop_dups = st.checkbox("Drop duplicate rows", value=True)

    with col_b:
        do_drop_const = st.checkbox("Drop constant columns", value=True)
        do_cap_outliers = st.checkbox("Cap ouliers (IQR method)", value=True)

    missing_threshold = st.slider(
        label="Drop columns where missing values exceed (%)",
        min_value=10,
        max_value=100,
        value=50,
        step=5,
        help=(
            "Columns with more missing data than this percentage will be removed. "
            "Default 50% means columns that are more than half empty are dropped. "
        )
    ) / 100.0
    # Divide by 100 to convert percentage to fraction (0.0 to 1.0)
    # Our cleaner functions expect a fraction, but sliders show percentages

    # --- SECTION 4: APPLY BUTTON --------------------------------------------------

    st.divider()

    apply_clicked = st.button(
        "Apply All Cleaning",
        type="primary",
        use_container_width=True
        # use_container_width=True stretches the button full width
    )

    if apply_clicked:
        with st.spinner("Applying transformations..."):
            cleaned = df.copy()
            # Start with a fresh copy of the raw data
            # This means clicking Apply multiple times always
            # produces the same result (idempotent behaviour)

            # Apply transformations in a logical order:
            # 1. Type detection first - fills need correct types
            # 2. Drop empty rows - remove garbage before filling
            # 3. Fill nulls - works on the remaining nulls
            # 4. Drop high-missing columns - after filling, re-check
            # 5. Drop duplicates - on cleaned data
            # 6. Drop constants - structural cleanup
            # 7. Cap outliers - after types are correct
            # 8. Normalize names - last, so audit log uses clean names

            if do_infer_types:
                cleaned = infer_and_convert_dtypes(cleaned, audit_log)

            if do_drop_empty:
                cleaned = drop_fully_empty_rows(cleaned, audit_log)

            if cols_with_nulls:
                cleaned = apply_fills(cleaned, user_fills, audit_log)

            cleaned = drop_high_missing_columns(
                cleaned, missing_threshold, audit_log
            )

            if do_drop_dups:
                cleaned = drop_duplicate_rows(cleaned, audit_log)

            if do_drop_const:
                cleaned = drop_constant_columns(cleaned, audit_log)

            if do_cap_outliers:
                cleaned = cap_outliers_iqr(cleaned, audit_log=audit_log)

            if do_normalize:
                cleaned = normalize_column_names(cleaned, audit_log)

            # Save the cleaned DataFrame
            st.session_state["cleaned_df"] = cleaned
            st.session_state["audit_log"] = audit_log

            # Computes quality score on the cleaned data
            quality = compute_quality_score(cleaned)
            st.session_state["quality"] = quality

        st.success("Cleaning complete!")

        # --- Before / After Metrics ---------------------------------------------------

        st.subheader("Results")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "Rows Before", f"{df.shape[0]:,}"
        )
        m2.metric(
            "Rows After",
            f"{cleaned.shape[0]:,}",
            delta=f"{cleaned.shape[0] - df.shape[0]:,}"
            # delta = shows a small colored number below the main value
            # Negative delta shows in red, positive in green
        )
        m3.metric("Cols Before", df.shape[1])
        m4.metric(
            "Cols After",
            cleaned.shape[1],
            delta=cleaned.shape[1] - df.shape[1]
        )

        # --- Quality Score --------------------------------------------------------

        st.subheader("Data Quality Score")

        q1, q2, q3 = st.columns(3)
        q1.metric("Overall Score", f"{quality['overall']}%")
        q2.metric("Grade", quality["grade"])
        q3.metric("Issues Found", quality["issue_count"])

        # Gauge chart
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=quality["overall"],
            title={"text": "Data Quality"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#2563eb"},
                "steps": [
                    {"range": [0, 60], "color": "#fecaca"},
                    # Light Red for poor quality
                    {"range": [60, 75], "color": "#fef08a"},
                    # Light yellow for acceptable
                    {"range": [75, 90], "color": "#bbf7d0"},
                    # Light green for good
                    {"range": [90, 100], "color": "#86efac"}
                    # Deeper green foe excellent
                ],
                "threshold": {
                    "line": {"color": "black", "width": 3},
                    "thickness": 0.75,
                    "value": quality["overall"]
                }
            }
        ))
        fig_gauge.update_layout(height=300)
        # updates_layout() modifies chart-level properties
        st.plotly_chart(fig_gauge, use_container_width=True)

        # Dimension breakdown bars
        dims = ["completeness", "uniqueness", "consistency", "validity"]
        for dim in dims:
            score = quality[dim]["score"]
            desc = quality[dim]["description"]
            color = (
                "normal" if score >= 75 else "off"
            )
            st.progress(
                int(score)/100,
                # st.progress() expects a float between 0.0 and 1.0
                text=f"**{dim.title()}**: {score}% - {desc}"
            )
        if quality["issues"]:
            st.subheader("Issues Detected")
            for issue in quality["issues"]:
                st.warning(issue)

        # --- Audiit Log ----------------------------------------------------
        st.subheader("Transformation History")

        log_df = audit_log.to_dataframe()

        if not log_df.empty:
            st.dataframe(log_df, use_container_width=True)

            st.download_button(
                label="Download Audi Log",
                data=audit_log.to_text_report(),
                file_name="audit_log.txt",
                mime="text/plain"
            )
            # st.download_button() renders a button that triggers
            # a file download in the browser
            # data = is the constant to download
            # file_name = is what appears in the user's downloads folder

        # --- Cleaned Data Preview ----------------------------------------

        st.subheader("Cleaned Data Preview")
        st.dataframe(cleaned.head(100), use_container_width=True)
