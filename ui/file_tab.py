import streamlit as st
# streamlit is the entire UI framework
# st is the universal alias - every UI component is st.something()

from pipeline.loader import (
    load_file,
    profile_dataframe,
    build_connection_url,
    create_db_engine,
    get_table_names,
    load_table_from_db,
    run_custom_sql
)
from pipeline.audit import AuditLog


def render():
    # Renders Tab 1: File Upload and Database Connection.

    # This function is called by app.py inside "with tab1:"
    # Everything inside render() appears only in Tab 1.

    # After successfully loading data, it saves the DataFrame
    # to st.session_state so other tabs can access it.

    st.header("Step 1 - Load Your Data")
    st.write(
        "Upload a CSV or Excel file, or connect directly to a database. "
        "Your data never leaves your machine - everything runs locally."
    )
    # st.write() renders text, markdown, or data
    # It is the most versatile display function in Streamlit

    # --- DATA SOURCE SELECTOR ------------------------------------------

    source = st.radio(
        label="Choose your data source",
        options=["Upload a File", "Connect to Database"],
        horizontal=True
        # horizontal=True renders the options side by side instead of stacking
    )
    # st.radio() creates a radio button group
    # Returns the currently selected option as a string

    st.divider()
    # Renders a horizontal line to visually separate sections

    # --- OPTION A: FILE UPLOAD --------------------------------------

    if source == "Upload a File":
        uploaded_file = st.file_uploader(
            label="Drag and drop a file here, or click to browse",
            type=["csv", "xlsx", "xls"],
            # type = restricts which file types appear in the browser picker
            help="Supported formats: CSV, Excel (.xlsx, .xls)"
            # help = shows a small tooltip question mark next to the widget
        )
        # Returns None if no file is uploaded
        # Returns a UploadedFile object if a file has been choosen

        if uploaded_file is not None:
            # Only try to process the file is one has been choosen

            # FIX: WHY THIS CHECK IS NEEDED
            #
            # Streamlit reruns the ENTIRE app script from top to bottom every
            # time the user interacts with ANY widget - including sidebar
            # sliders on the Insights tab, checkboxes on the Clean tab, etc.
            #
            # WITHOUT this check, the flow on every rerun was:
            # 1. User moves a slider on Insights tab
            # 2. Streamlit reruns app.py
            # 3. file_tab.render() runs again
            # 4. uploaded_file is still not None (file is still "uploaded")
            # 5. load_file() runs again, _save_dataframe_to_session() runs
            # 6. _save_dataframe_to_session() sets cleaned_df = None
            # 7. Insights tab now sees cleaned_df = None -> shows warning
            #
            # WITH this check:
            #   - We compare the current file's name to what's already stored
            #     in session_state["source_name"]
            #   - If they match, the file is already loaded -> skips reload
            #   - If they differ, it's a new file -> load it fresh
            #
            # This means _save_dataframe_to_session() (which resets cleaned_df)
            # only runs when a genuinely new file is uploaded, not on every
            # widget interaction anywhere in the app.

            if st.session_state.get("source_name") != uploaded_file.name:
                # st.session_state.get("source_name") safety retrieves the same
                # of the last loaded file. Returns None is no file loaded yet.
                # If the name matches the current file, we skip reloading.

                with st.spinner("Reading file..."):
                    # st.spinner() shows a loading animation while the code inside runs
                    # "with" is a context manager - the spinner shows for the duration

                    try:
                        df = load_file(uploaded_file)
                        # Call our loader function - returns a DataFrame

                        _save_dataframe_to_session(df, uploaded_file.name)
                       # Save to session state using out helper below

                        st.success(
                            f" Loaded **{uploaded_file.name}** - "
                            f"{df.shape[0]:,} rows x {df.shape[1]} columns"
                        )
                        # :, inside f-string formats numbers with commas
                        # 10000 becomes 10,000

                    except Exception as e:
                        st.error(f"Failed to load file: {e}")
                        # st.error() shows a red error banner
                        # str(e) converts the exception to a readable message
                        return
                    # Stop rendering if loading failed

     # --- OPTION B: DATABASE CONNECTION ------------------------------
    else:
        st.subheader("Database Connection")

        col1, col2 = st.columns(2)
        # st.columns(2) creates a 2-column layout
        # returns column object you render widgets inside

        with col1:
            db_type = st.selectbox(
                "Database type",
                options=["PostgreSQL", "MySQL", "SQLite"]
            )
            # st.selectbox() renders a dropdown
            # Returns the currently selected string

            host = st.text_input("Host", value="localhost")
            # st.text_input() renders a single-line text box
            # value = sets the default text already in the box

            port_defaults = {"PostgreSQL": "5432",
                             "MySQL": "3306", "SQLite": ""}
            port = st.text_input("Port", value=port_defaults.get(db_type, ""))
            # Show the default port for the selected database type

        with col2:
            database = st.text_input(
                "Database name",
                placeholder="e.g. my_database"
                # placeholder = shows grey hint text when the box is empty
            )
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            # type = "password" replaces characters with dots while typing

        connect_clicked = st.button("Connect", type="primary")
        # st.button() renders a clickable button
        # Returns True only on the exact frame when the button is clicked
        # type = "primary" styles it as the main action (filled blue)

        if connect_clicked:
            if db_type != "SQLite" and not all([host, database, username]):
                # all([...]) returns True only if every item is truthy
                # Empty strings are falsy, so this catches unfilled fields
                st.error("Please fill in all required connecciton fields.")
            else:
                with st.spinner("Connecting to database..."):
                    try:
                        url = build_connection_url(
                            db_type, host, port, database, username, password
                        )
                        engine = create_db_engine(url)
                        tables = get_table_names(engine)

                        st.session_state["db_engine"] = engine
        # Store the engine so we do not reconnect on every interaction
                        st.session_state["db_tables"] = tables

                        st.success(
                            f"Connected to {db_type}."
                            f"Found {len(tables)} table(s)."
                        )
                    except Exception as e:
                        st.error(f"Connection failed: {e}")

        # Show table selector only after successful connection
        if "db_engine" in st.session_state:
            st.subheader("Load Data")

            selected_table = st.selectbox(
                "Select a table",
                options=st.session_state['db_tables']
            )

            custom_sql = st.text_area(
                "Or write a custom SQL query (optional)",
                placeholder="SELECT * FROM orders WHERE status = 'completed'",
                height=100
                # height = sets the text area height in pixels
            )

            row_limit = st.number_input(
                "Row limit",
                min_value=100,
                max_value=1000000,
                value=100000,
                step=10000
                # number_input renders a numeric input with up/down arrows
            )

            load_clicked = st.button("Load Data", type="primary")

            if load_clicked:
                with st.spinner("Loading data from Database..."):
                    try:
                        engine = st.session_state["db_engine"]

                        if custom_sql.strip():
                            # .strip() removes whitespaces
                            #  empty after strip = no query entered
                            df = run_custom_sql(engine, custom_sql)

                        else:
                            df = load_table_from_db(
                                engine, selected_table, limit=row_limit
                            )

                        source_name = (
                            f"SQL Query" if custom_sql.strip()
                            else selected_table
                        )
                        _save_dataframe_to_session(df, source_name)

                        st.success(
                            f"Loaded {df.shape[0]:,} rows x "
                            f"{df.shape[1]} columns from {source_name}"
                        )
                    except Exception as e:
                        st.error(f"Failed to load data: {e}")

    # --- DATA PREVIEW (shown after any successful load) ------------------

    if "df" in st.session_state:
        # Check if data has been loaded before showing preview

        df = st.session_state['df']

        st.divider()
        st.subheader("Data Preview")

        preview_rows = st.slider(
            "Number of rows to Preview",
            min_value=5,
            max_value=min(500, len(df)),
            # min() ensures we don't show more rows that exist
            value=min(50, len(df))
        )

        st.dataframe(df.head(preview_rows), use_container_width=True)
        # st.dataframe() renders an interactive scrollable table
        # use_container_width=True stretches it to fill the full page width

        st.divider()
        st.subheader("Column Profile")
        st.caption(
            "Review data types, missing values, and unique counts"
            "before cleaning"
        )
        # st.caption() renders small grey helper text

        profile = profile_dataframe(df)
        st.dataframe(
            profile.style.background_gradient(
                subset=["missing_%"],
                cmap="RdYlGn_r"
                # background_gradient colors cells based on value
                # "RdYlGn_r" red (high missing) to green (low missing) reversed
            ),
            use_container_width=True
        )


def _save_dataframe_to_session(df, source_name: str):
    # Saves the loaded DataFrame to session stats and resets related state.

    # This is a private helper function (indicated by the _prefix convention).
    # It is called both by the file upload path and database path,
    # so we avoid repeating the same code in both places (DRY principle -
    # Don't Repeat Yourself),

    # We reset the audit log whenever new data is loaded because
    # the previous log refers to a different dataset.

    st.session_state["df"] = df.copy()
    # Store the raw loaded data

    st.session_state["raw_df"] = df.copy()
    # Also store a clean copy for potential "reset to original" feature

    st.session_state["source_name"] = source_name
    # Remember where the data came for display in other tabs

    st.session_state["cleaned_df"] = None
    # Clear any previously cleaned data - it belongs to the old dataset

    st.session_state["audit_log"] = AuditLog()
    # Fresh audit log for the new dataset

    st.session_state["quality"] = None
    # Clear quality score from previous dataset
