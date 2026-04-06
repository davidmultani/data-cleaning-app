# This is the entry point. Streamlit runs this file when you do: streamlit run app.py

from ui import file_tab, clean_tab, insights_tab, export_tab
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from .env file
# This must run before any code that os.getenv()
load_dotenv()

# Import all four tab modules

# ---------------------- PAGE CONFIGURATION -------------------------------------------
# This must be the FIRST Streamlit command called in the script
# Calling any other st.function before this raises an error

st.set_page_config(
    page_title="Data Cleaning Pipeline",
    # Sets the browser tab title

    page_icon="🧹",
    # Sets the favicon - emoji works here

    layout="wide",
    # "wide" uses full browser width
    # Default "centered is a narrow column in the middle"

    initial_sidebar_state="expanded"
    # Start with the sidebar visible (it contains chart controls)
)

# ----------------------------- CUSTOM CSS -------------------------------------------
# Streamlit allows injecting CSS to customise the look
# This overrides some default Streamlit styles

st.markdown("""
            <style>
            /* Reduce top padding on main content area */
            .block-container {
            padding-top: 2rem;
            }
            /* Make metric values slightly larger */
            [data-testid="metric-container"] {
            background-color: #f8fafc;
            border: 1px solid #e2e8f0;
            padding: 1rem;
            border-radius: 0.5rem;
            }
            </style>
            """, unsafe_allow_html=True)
# unsafe-allow-html=True is required to inject HTML/CSS
# Only use this for your own CSS - never inject user-provided HTML


# ------------------------- APP HEADER -------------------------------------------

st.title("🧹 Data Cleaning & Insights Pipeline")
st.caption(
    "Upload any CSV or Excel file -> clean it -> explore visually - export anywhere"
)

# ------------------------ STATUS BAR --------------------------------------------
# Show a compact summary of current state at the top of every tab

if "df" in st.session_state and st.session_state["df"] is not None:
    df = st.session_state["df"]

    raw_info = (
        f"📂 **Loaded:** {st.session_state.get('source_name', 'file')}"
        f"({df.shape[0]:,} rows x {df.shape[1]} cols)"
    )

    if ("cleaned_df" in st.session_state and
            st.session_state["cleaned_df"] is not None):
        cdf = st.session_state["cleaned_df"]
        raw_info += (
            f" | 🧹**Cleaned:** {cdf.shape[0]:,} rows x {cdf.shape[1]} cols"
        )

    if ("quality" in st.session_state and
            st.session_state["quality"] is not None):
        score = st.session_state["quality"]["overall"]
        grade = st.session_state["quality"]["grade"]
        raw_info += f" | 📊 **Quality:** {score}% (Grade {grade})"

    st.info(raw_info)

# ------------------------------ TABS ------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs([
    "📂  Upload",
    "🧹  Clean",
    "📊  Insights",
    "💾  Export"
])
# st.tabs() creates the tab navigation bar
# Returns one context object per tab label
# The strings become the tab labels

with tab1:
    file_tab.render()
    # Everything inside "with tab1:" renders only in the first tab
    # render() contains all the UI code for that tab

with tab2:
    clean_tab.render()

with tab3:
    insights_tab.render()

with tab4:
    export_tab.render()
