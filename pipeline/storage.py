import os
import pandas as pd
from io import BytesIO

# We use lazy imports for Azure inside the function
# so the app still works even if someone hasn't installed the Azure SDK


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    # Converts a DataFrame to CSV bytes ready for download or upload.

    # We use BytesIO as a buffer:
    # 1. Write the CSV into the buffer (in memory)
    # 2. Read the bytes back out

    # This avoids writing a temporary files to disk.

    buffer = BytesIO()
    # BytesIO is an in-memory byte stream - acts like a file but lives in RAM

    df.to_csv(buffer, index=False, encoding="utf-8")
    # Write CSV into the buffer
    # index=False excludes the row numbers (0, 1, 2,....) from the output

    buffer.seek(0)
    # .seek(0) rewinds the buffer to the start
    # Without this, reading would start from the end (giving empty bytes)

    return buffer.getvalue()
    # .getvalue() returns all the bytes in the buffer


def save_locally(df: pd.DataFrame, file_path: str) -> None:
    # Saves the DataFrame as a CSV file to the local filesystem.
    # Creates any necessary parent directories if they don't exists.

    parent_dir = os.path.dirname(file_path)
    # os.path.dirname() extracts the dictionary part from the full path
    # For "/home/user/output/clean.csv" it returns "/home/user/output"

    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
        # os.makedirs() creates the dictionary and all missing parents
        # exist_ok=True prevents an error if the directory already exists.

    df.to_csv(file_path, index=False, encoding="utf-8")


def upload_to_adls(df: pd.DataFrame,
                   account_name: str,
                   account_key: str,
                   container_name: str,
                   file_path: str) -> None:
    # Uploads the DataFrame as a CSV to Azure Data Lake Storage Gen2.
    # Parameters:
    #   account_name - the name of your Azure Storage Account.
    #   account_key - the account access key from Azure Portal.
    #   container_name - the filesystem/container name in ADLS
    #   file_path - the target path inside the container
    #               e.g., "cleaned/2024/january/data.csv"

    #   The function creates or overwrites the file at file_path.

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        # Import here (not at top) so that app works without Azure SDK
        # if the user only needs local funcionality

    except ImportError:
        raise ImportError(
            "azure-storage-file datalake is not installed. "
            "Run: pip install azure-storage-file-datalake"
        )

    # Build the ADLS Gen2 endpoint URL
    account_url = f"https://{account_name}.dfs.core.windows.net"
    # .dfs.core.windows.net is the ADLS Gen2 specific endpoint
    # (different from .blob.core.windows.net which is regular Blob storage)

    service_client = DataLakeServiceClient(
        account_url=account_url,
        credential=account_key
        # credential can also be a SAS token, a ClientSecretCredential,
        # or a DefaultAzureCredential for production workloads
    )
    # This creates the top-level client connected to your storage account.

    filesystem_client = service_client.get_file_system_client(
        file_system=container_name
    )
    # Gets a client representing the specific container/filesystem
    # In ADLS Gen2, "filesystem" = "container" - same thing, different names

    file_client = filesystem_client.get_file_client(file_path)
    # Gets a client representing the specific file location
    # The file does not need to exist yet

    csv_bytes = dataframe_to_csv_bytes(df)
    # Converts the DataFrame to bytes using our helper function above

    file_client.upload_data(csv_bytes, overwrite=True, length=len(csv_bytes))
    # .upload_data() sends the bytes to Azure
    # overwrite=True replaces the file if it already exists.
    # length= is recommended for large files - tells Azure the exact size upfront


def get_credentials_from_env() -> dict:
    # Reads ADLS credentials from environment variables.

    # In local development these come from your .env file loaded bby dotenv.
    # On Streamlit Cloud they come from the secrets manager.

    # Returns a dictionary with the credential values,
    # or None for any value that is not set.

    return {
        "account_name": os.getenv("ADLS_ACCOUNT_NAME"),
        "account_key": os.getenv("ADLS_ACCOUNT_KEY")
    }
    # os.getenv() returns the value of an environment variable
    # Returns None if the variable is not set (safer than os.environ[key]
    # which throws a KeyError if the variable is missing)
