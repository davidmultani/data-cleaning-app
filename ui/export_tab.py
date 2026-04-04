import streamlit as st
import os
from dotenv import load_dotenv

from pipeline.storage import (
    dataframe_to_csv_bytes,
    save_locally,
    upload_to_adls,
    get_credentials_from_env
)

load_dotenv()
# Loads .env file into environment variables
# Must be called before get_credentials_from_env()


def render():
    st.header("Step 4 - Export Your Data")

    if ("cleaned_df" not in st.session_state or
            st.session_state["cleaned_df"] is None):
        st.warning("⚠️ Please clean your data first.")
        return

    df = st.session_state["cleaned_df"]

    st.info(
        f"Ready to export: **{df.shape[0]:,} rows x {df.shape[1]} columns**"
    )

    # Create two tabs inside the Export tab
    local_tab, cloud_tab = st.tabs(["💾 Local Download", "☁️ Azure Data Lake"])

    # ------------------------ LOCAL DOWNLOAD -----------------------------

    with local_tab:
        st.subheader("Download to Your Computer")
        st.write(
            "Click the button below to download the cleaned data "
            "as a CSV file."
        )

        csv_bytes = dataframe_to_csv_bytes(df)
        # Convert DataFrame to bytes once - reuse for the button below

        source_name = st.session_state.get("source_name", "data")
        # Get the original file/table name to use in the download filename

        default_filename = (
            source_name.replace(".csv", "").replace(".xlsx", "")
            + "_cleaned.csv"
        )
        # Build a sensible default filename like "sales_data_cleaned.csv"

        download_filename = st.text_input(
            "File name",
            value=default_filename
        )
        # Let the user customize the filename before downloading

        st.download_button(
            label="⬇️ Download Cleaned CSV",
            data=csv_bytes,
            file_name=download_filename,
            mime="text/csv",
            use_container_width=True
        )

        st.divider()

        # Also offer audit log download
        if ("audit_log" in st.session_state and
                st.session_state["audit_log"] is not None):

            audit_log = st.session_state["audit_log"]

            if not audit_log.to_dataframe().empty:
                st.write("**Also download the transformation audit log:**")
                st.download_button(
                    label="📄 Download Audit Log (.txt)",
                    data=audit_log.to_text_report(),
                    file_name="audit_log.txt",
                    mime="text/plain"
                )

    # ---------------------- AZURE DATA LAKE UPLOAD ----------------------

    with cloud_tab:

        st.subheader("Upload to Azure Data Lake Storage Gen2")
        st.write(
            "Upload the cleaned dataset directly to your Azure Data Lake. "
            "You will need an Azure Storage Account with ADLS Gen2 enabled."
        )

        # Try to pre-fill from environment variables
        env_creds = get_credentials_from_env()

        with st.expander("🔐 Azure Credentials", expanded=True):

            account_name = st.text_input(
                "Storage Account Name",
                value=env_creds.get("account_name") or "",
                help="The name of your Azure Storage Account (not the full URL)"
            )

            account_key = st.text_input(
                "Account Key or SAS Token",
                value=env_creds.get("account_key") or "",
                type="password",
                help=(
                    "Find this in Azure Portal -> "
                    "Storage Account -> Access Keys"
                )
            )

        with st.expander("📁 Target Location", expanded=True):

            container_name = st.text_input(
                "Container / Filesystem Name",
                placeholder="e.g. raw-data",
                help=(
                    "The container inside your storage account "
                    "where the file will be saved"
                )
            )

            file_path = st.text_input(
                "Target File Path",
                value="cleaned/data.csv",
                help=(
                    "The path inside the container. "
                    "You can use folders: 'folder/subfolder/file.csv'"
                )
            )

        upload_clicked = st.button(
            "☁️ Upload to Azure",
            type="primary",
            use_container_width=True
        )

        if upload_clicked:
            missing_fields = [
                name for name, val in [
                    ("Storage Account Name", account_name),
                    ("Account Key", account_key),
                    ("Container Name", container_name),
                    ("File Path", file_path)
                ]
                if not val.strip()
                # List comprehension to find which required fields are empty
            ]

            if missing_fields:
                st.error(
                    f"Please fill in: {', '.join(missing_fields)}"
                )
            else:
                with st.spinner("Uploading to Azure Data Lake..."):
                    try:
                        upload_to_adls(
                            df=df,
                            account_name=account_name.strip(),
                            account_key=account_key.strip(),
                            container_name=container_name.strip(),
                            file_path=file_path.strip()
                        )

                        st.success(
                            f"✅ Successfully uploaded to "
                            f"**{container_name}/{file_path}**"
                        )
                        st.balloons()
                        # st.balloons() shows a fun animation on success

                    except Exception as e:
                        st.error(f"❌ Upload failed: {e}")
                        st.info(
                            "Check your credentials and make sure the "
                            "container exists in your storage account."
                        )
