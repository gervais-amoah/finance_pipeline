import pandas as pd
import sqlite3
import logging
from typing import Optional
from pathlib import Path
from tabulate import tabulate

# Path configuration
DB_PATH = Path("database/forex_data.db")
RAW_CSV_FILE_PATH = Path("data/raw/daily_forex_rates.csv")
PROCESSED_FILES_PATH = Path("data/processed")

# SQL table name
HISTORY_TABLE_NAME = "forex_rates_history"


# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def ensure_directories() -> bool:
    """Ensure required directories exist.

    Returns:
        bool: True if directories were created or already exist
    """
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        PROCESSED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logging.error(f"❌ Error creating directories: {e}")
        return False


def process_raw_csv_file(months: int = 2) -> Optional[Path]:
    """
    Get data from the raw CSV file for the specified number of months and save it to the processed directory.

    Args:
        months: Number of months of data to extract (default: 2)

    Returns:
        Path to the processed CSV file if successful, None otherwise
    """
    try:
        logging.info(f"⌛ Processing raw CSV file: {RAW_CSV_FILE_PATH}")
        # Read the raw CSV file
        df = pd.read_csv(RAW_CSV_FILE_PATH)

        # Convert 'date' column to datetime format
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        today = pd.Timestamp.today()
        start_date = today - pd.DateOffset(months=months)

        # Filter data for the specified date range
        df_recent = df[(df["date"] >= start_date) & (df["date"] <= today)]

        output_csv = PROCESSED_FILES_PATH / f"forex_rates_{months}m.csv"

        # Save filtered data
        df_recent.to_csv(output_csv, index=False)

        logging.info(
            f"✅ Raw file processed successfully. {len(df_recent)} rows extracted for {start_date.date()} to {today.date()}."
        )

        return output_csv

    except FileNotFoundError:
        logging.error(f"❌ Raw CSV file not found at {RAW_CSV_FILE_PATH}")
        return None

    except Exception as e:
        logging.error(f"❌ Error processing raw CSV file: {e}")
        return None


def transform_data(path: Path) -> pd.DataFrame:
    """Clean and format DataFrame.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    try:
        df = pd.read_csv(path)
        if df.empty:
            logging.warning("⚠️ Loaded DataFrame is empty.")
            return pd.DataFrame()

        logging.info("⌛ Transforming data...")
        transformed_df = df.copy()

        transformed_df.drop_duplicates(inplace=True)
        transformed_df.dropna(
            subset=["currency", "exchange_rate", "date"], inplace=True
        )
        transformed_df["date"] = pd.to_datetime(transformed_df["date"], errors="coerce")
        transformed_df = transformed_df[transformed_df["exchange_rate"] > 0]

        # Add timestamp_utc based on 10:00:00 UTC
        transformed_df["timestamp_utc"] = transformed_df["date"] + pd.Timedelta(
            hours=10
        )
        logging.info(f"✅ {len(transformed_df)} rows after transforming.")
        return transformed_df
    except AttributeError as e:
        logging.error(f"❌ Error checking DataFrame: {e}")
        return pd.DataFrame()


def create_table(conn: sqlite3.Connection) -> bool:
    """Create SQL table if it doesn't exist.

    Args:
        conn: SQLite connection
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        currency TEXT NOT NULL,
        base_currency TEXT NOT NULL,
        currency_name TEXT,
        exchange_rate REAL NOT NULL,
        date DATE NOT NULL,
        timestamp_utc TEXT NOT NULL,
        UNIQUE(currency, date)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"✅ Table {HISTORY_TABLE_NAME} created or already exists.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error creating table: {e}")
        return False


def insert_data(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Insert data into SQLite database.

    Args:
        conn: SQLite connection
        df: DataFrame containing data to insert

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    logging.info("⌛ Inserting data into SQL database...")
    inserted = 0
    skipped = 0

    # More efficient batch insert using to_sql
    try:
        # Create a temporary DataFrame with formatted dates
        insert_df = df.copy()
        insert_df["date"] = insert_df["date"].dt.strftime("%Y-%m-%d")
        insert_df["timestamp_utc"] = insert_df["timestamp_utc"].dt.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        # Use to_sql with if_exists='append' and index=False
        insert_df.to_sql(
            HISTORY_TABLE_NAME,
            conn,
            if_exists="append",
            index=False,
            method="multi",  # Faster for multiple rows
        )

        # Get count of inserted rows
        cursor = conn.cursor()
        cursor.execute(f"SELECT changes()")
        inserted = cursor.fetchone()[0]

    except Exception as e:
        logging.error(f"❌ Batch insert failed: {e}")
        logging.info("⌛ Falling back to row-by-row insertion...")

        # Fallback to row-by-row insertion
        for _, row in df.iterrows():
            try:
                conn.execute(
                    f"""
                    INSERT OR IGNORE INTO {HISTORY_TABLE_NAME}
                    (currency, base_currency, currency_name, exchange_rate, date, timestamp_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.currency,
                        row.base_currency,
                        row.currency_name,
                        row.exchange_rate,
                        row.date.strftime("%Y-%m-%d"),
                        row.timestamp_utc.isoformat(),
                    ),
                )
                inserted += conn.total_changes
            except Exception as e:
                skipped += 1
                logging.warning(f"⚠️ Row skipped: {e}")

    conn.commit()
    logging.info(
        f"✅ Insertion completed: {inserted} new rows inserted, {skipped} rows skipped."
    )
    return inserted


def display_data(conn: sqlite3.Connection) -> None:
    """Display sample of the inserted data.

    Args:
        conn: SQLite connection
    """
    logging.info("Displaying recent data (limited to 10 rows):")
    query = f"""
        SELECT currency, base_currency, exchange_rate, timestamp_utc
        FROM {HISTORY_TABLE_NAME}
        ORDER BY timestamp_utc DESC, currency ASC
        LIMIT 10;
    """
    df = pd.read_sql_query(query, conn)
    print(tabulate(df, headers="keys", tablefmt="fancy_grid"))


def save_to_database(df: pd.DataFrame) -> bool:
    """Save the data to SQLite database using smaller function calls.

    Args:
        df: DataFrame to save

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            if not create_table(conn):
                return False

            rows_inserted = insert_data(conn, df)
            if rows_inserted > 0:
                display_data(conn)
                return True
            return False

    except sqlite3.Error as e:
        logging.error(f"❌ Database connection error: {e}")
        return False


def run() -> None:
    """Main function to orchestrate the data processing pipeline."""
    logging.info(f"⚙️ Starting ETL:CSV pipeline with {RAW_CSV_FILE_PATH}")

    if not ensure_directories():
        logging.error("❌ Failed to create necessary directories. Exiting.")
        return

    processed_csv_file = process_raw_csv_file()
    if not processed_csv_file:
        logging.error("❌ Failed to process CSV file. Exiting.")
        return

    df = transform_data(processed_csv_file)

    if df is not None:
        db_success = save_to_database(df)

        if db_success:
            logging.info("✅ ETL:CSV process completed successfully.")
        else:
            logging.warning("⚠️ ETL:CSV process completed with warnings.")
    else:
        logging.error("❌ ETL:CSV process failed during transformation.")


if __name__ == "__main__":
    run()
