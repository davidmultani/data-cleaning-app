import chardet
# Detects character encoding of raw bytes without this,
# files with special characters (accents, symbols) would -
# Either fail to load or show garbage characters.

import pandas as pd
from io import BytesIO
# BytesIO wras raw bytes so they behave like a file on disk
# pandas read_csv() expects a file-like object, not raw bytes
# Streamlit gives us raw bytes, so we need this wrapper

from sqlalchemy import create_engine, text, inspect
# create_engine - creates a database connection pool
# text - wraps raw SQL strings safely
# inspect - reads database metadata (table names, column names)


### --- FILE LOADING --- ###
def detect_encoding(file_bytes: bytes) -> str:
    # Analyzes raw bytes and returns the most likely character encoding.

    result = chardet.detect(file_bytes)
    # chardet.detect() returns a dict like:
    # {"encoding":"utf-8", "confidence":0.99, "language":""}

    encoding = result.get("encoding", "utf-8")
    # .get("encoding", "utf-8") safely retrieves the encoding key
    # If chardet failed to detect anything, "utf-8" is the fallback
    # because it is the most common encoding for modern files

    return encoding or "utf-8"


def load_file(file_obj) -> pd.DataFrame:
    # Reads an uploaded file (CSV or Excel) and return a DataFrame.

    # file_obj is the object returned by streamlit's file_uploader widget.
    # It has two important attributes:
    # .read() - returns the raw bytes of the file
    # .name - returns the original filename as a string

    file_obj.seek(0)
    file_bytes = file_obj.read()
    if file_obj.size > 500_000_000:
        df = pd.read_csv(BytesIO(file_obj), chunksize=100000)
        df = pd.concat(df)
        return df

    # .read() loads the entire file content into memory as bytes
    # we store it in file_bytes because we need it twice:
    # once for encoding detection and once for reading into pandas

    if isinstance(file_bytes, str):
        file_bytes = file_bytes.encode("utf-8")

    if len(file_bytes) == 0:
        raise ValueError(
            "The file appears to be empty. "
            "Please try uploading it again. "
        )

    filename = file_obj.name
    # Gets the original filename like "sales_data.csv"
    # We use this to determine which reader to use

    # Defining all the values that should be treated as missing
    na_values = [
        "NULL", "null", "Null",
        "N/A", "n/a", "NA", "na",
        "None", "none", "NONE",
        "-", "--", "?", ""
    ]
    # When pandas reads the file, any cell containing these strings
    # will be converted to NaN (pandas missing value representation)
    # Without this, "NULL" would be read as the string "NULL"

    if filename.endswith(".csv"):
        encoding = detect_encoding(file_bytes)
        # Detect encoding first so we can tell pandas which one to use

        df = pd.read_csv(BytesIO(file_bytes),
                         # BytesIO(file_bytes) wraps the bytes as a file_like object
                         # pandas can then read it exactly like it reads a file on disk

                         encoding=encoding,
                         # Tell pandas which character encoding to use

                         na_values=na_values,
                         # Tells pandas which strings to treat as missing values

                         low_memory=False
                         # low_memory=False tells pandas to read the whole file
                         # before inferring columns types, giving more accurate dtypes
                         # Default True can cause "mixed types" warnings on large files
                         )
    elif filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(
            BytesIO(file_bytes),
            na_values=na_values
            # Excel file have their own encoding internaly
            # openpyxl handles it automatically, no need to detect
        )
    else:
        raise ValueError(
            f"Unsupported file type: {filename}. "
            "Please upload a CSV or Excel file."
        )
        # raise stop execution and sends their error message
        # The UI catches this and shows it to the user

    return df


def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Generates a summary table describing every column
    # Returns a new DataFrame where each row describes one column
    # from the input Dataframe. Ths is used in the UI to show
    # the user a quick oberview of their data before cleaning.

    rows = []
    # We will build a list of dictionaries, one per column
    # then convert the whole list into a DataFrame at the end
    # This is more efficient than building the DataFrame row by row

    for col in df.columns:
        # df.columns gives us the list of all column names
        # We loop through each one to compute its statistics

        series = df[col]
        # df[col] selects one column as a pandas Series
        # Storing it in a variable avoids repeating df[col] everywhere

        missing_count = series.isna().sum()
        # .isna() returns a boolean Series: True where value is NaN
        # .sum() counts the True values (True=1, False=0)

        rows.append({
            "column": col,
            "dtype": str(series.dtype),
            # str() converts the dtype object to a readable string
            # like "int64", "float64", "object", "datetime64[ns]"

            "total_rows": len(df),
            "non_null": series.notna().sum(),
            # .notna() is the opposite of .isna()

            "missing_count": missing_count,
            "missing_%": round(100 * missing_count / len(df), 1),
            # Percentage of rows that are missing in this column
            # round(.... , 1) keeps one decimal place

            "unique_values": series.nunique(),
            # .nunique() counts distinct non-null values

            "sample_value": (str(series.dropna().iloc[0])
                             if series.notna().any() else "-"),
            # Get the forst non-null value as a simple to show the user
            # .dropna() rmeoves NaN values from the series
            # .iloc[0] gets the first remaining value bby position
            # The "if ... else" gaurd prevents crashing on all-null columns

        }
        )
    return pd.DataFrame(rows)
    # Convert the list of dictionaries into a DataFrame
    # Dictionary keys become column names automatically

### --- DATABASE LOADING --- ###


def build_connection_url(db_type: str, host: str, port: str,
                         database: str, username: str,
                         password: str) -> str:
    # Builds a SQLAlchemy connection URL string of the given database type.
    # The connection URL format is:
    #   dialect+driver://username:password@host:port/database

    # SQLAlchemy uses the string to know:
    # - Which databse software to connect to (dialect)
    # - Which Python Driver to use (driver)
    # - Where to connect (host, port)
    # - What credentials to use (username, password)
    # - Which databse to open (database)

    if db_type == "PostgreSQL":
        return (f"postgresql+psycopg2://"
                f"{username}:{password}@{host}:{port}/{database}")
        # postgresql = the dialect
        # psycopg2 = the driver (psycopg2-binary package)

    elif db_type == "MySQL":
        return (f"mysql+pymysql://"
                f"{username}:{password}@{host}:{port}/{database}")
        # pymysql = the driver (the pymysql package)

    elif db_type == "SQLite":
        return f"sqlite:///{database}"
        # SQLite is the local file, no host/post/credentials needed
        # database is the file path like "mydata.db"

    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def create_db_engine(connection_url: str):
    # Creates a SQLAlchemy engine from the connection URL.

    # The engine is not a connection i it is a connection factory.
    # It manages a pool of connections, reusing them efficiently.
    # Actual connections are only opened when you run a query.

    engine = create_engine(
        connection_url,
        pool_pre_ping=True
        # pool_pre_ping=True tests the connection before using it
        # This prevent errors if the databse server dropped idle connections.
    )
    return engine


def get_table_names(engine) -> list:
    # Returns a list of all tables names in the connected database.
    # Used to populate the table selection dropdown in the UI.

    inspector = inspect(engine)
    # inspect() creates an Inspector object that can read
    # database metadata without loading any actual data

    return inspector.get_table_names()
    # Returns something like ["customers", "orders", "products"]


def load_table_from_db(engine, table_name: str,
                       limit: int = 100000) -> pd.DataFrame:
    # Loads all rows from a database table into a DataFrame.
    # limit protects you from accidentally loading a table with
    # 50 million rows and crashing your machine, Default is 100000
    # which is large enough for most analytical work.
    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
    # text() marks this as a raw SQL string
    # :limit is a bind parameter - SQLAlchemy replaces it safely
    # This prevents SQL injection attacks

    with engine.connect() as conn:
        # "with" ensures the connection is properly closed afterwards
        # even if an error occurs
        df = pd.read_sql(query, conn, params={"limit": limit})
        # pd.read_sql() runs the query and returns the result as a DataFrame

    return df


def run_custom_sql(engine, sql: str) -> pd.DataFrame:
    # Runs a user-provided SQL query and returns the result as a DataFrame.
    # This is the most powerful feature - the user can write any SELECT
    # query to pull exactly the data they need.

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df
