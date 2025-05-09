import pandas as pd
import requests
import sqlite3
import pytz

from datetime import datetime
from typing import Dict, Optional, Any
from tabulate import tabulate

from etl.config import (
    logging,
    DB_PATH,
    CSV_FILE_PATH,
    API_URL,
    DEFAULT_CURRENCY,
    API_TABLE_NAME,
    API_UPDATE_TIME,
    CET_TIMEZONE,
)


from services.supabase import sync_data


def ensure_directories() -> bool:
    """Ensure all required directories exist.

    Returns:
        bool: True if all directories were created or already exist
    """
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        CSV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logging.error(f"❌ Error creating directories: {e}")
        return False


def fetch_forex_data() -> Optional[Dict[str, Any]]:
    """Fetch forex data from the Frankfurter API.

    Returns:
        Optional[Dict[str, Any]]: Raw JSON response or None if failed
    """
    params = {"base": DEFAULT_CURRENCY}

    try:
        logging.info(f"⌛ Fetching data from API...")
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        logging.info("✅ Data retrieved successfully from API.")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"❌ Error retrieving data from API: {e}")
        return None


def transform_forex_data(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Transform raw API data into a structured DataFrame.

    Args:
        raw_data: JSON response from API

    Returns:
        Optional[pd.DataFrame]: Transformed DataFrame or None if failed
    """
    try:
        rates = raw_data.get("rates", {})
        date_str = raw_data.get("date", str(datetime.now().date()))

        # Build CET datetime at 16:00
        cet_dt = CET_TIMEZONE.localize(
            datetime.combine(
                datetime.strptime(date_str, "%Y-%m-%d"),
                API_UPDATE_TIME,
            )
        )

        # Convert to UTC
        utc_dt = cet_dt.astimezone(pytz.utc)
        utc_timestamptz = utc_dt.isoformat()

        # Create DataFrame
        df = pd.DataFrame(rates.items(), columns=["currency", "exchange_rate"])
        df["base_currency"] = DEFAULT_CURRENCY
        df["date"] = date_str
        df["timestamptz"] = utc_timestamptz

        logging.info(
            f"✅ JSON data transformed to DataFrame successfully. Rows: {len(df)}"
        )
        return df

    except ValueError as e:
        logging.error(f"❌ Error transforming data: {e}")
        return None


def save_to_csv(df: pd.DataFrame) -> bool:
    """Save the data to CSV file.

    Args:
        df: DataFrame to save

    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        if CSV_FILE_PATH.exists():
            df.to_csv(CSV_FILE_PATH, mode="a", header=False, index=False)
            logging.info(f"✅ Data appended to {CSV_FILE_PATH}")
        else:
            df.to_csv(CSV_FILE_PATH, mode="w", header=True, index=False)
            logging.info(f"✅ File {CSV_FILE_PATH} created with data.")
        return True
    except Exception as e:
        logging.error(f"❌ Error saving to CSV: {e}")
        return False


def create_table(conn: sqlite3.Connection) -> bool:
    """Create the database table if it doesn't exist.

    Args:
        conn: SQLite connection

    Returns:
        bool: True if table created or already exists, False on error
    """
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {API_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency TEXT,
            base_currency TEXT,
            exchange_rate REAL,
            date TEXT,
            timestamptz TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(currency, timestamptz)
        )
    """

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"✅ Table {API_TABLE_NAME} created or already exists.")
        return True
    except sqlite3.Error as e:
        logging.error(f"❌ Error creating table: {e}")
        return False


def insert_data(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
) -> int:
    """Insert data into the database table.

    Args:
        conn: SQLite connection
        df: DataFrame with data to insert

    Returns:
        int: Number of rows inserted
    """
    insert_query = f"""
        INSERT OR IGNORE INTO {API_TABLE_NAME} 
        (currency, base_currency, exchange_rate, date, timestamptz)
        VALUES (?, ?, ?, ?, ?)
    """

    data_to_insert = [
        (
            row["currency"],
            row["base_currency"],
            row["exchange_rate"],
            row["date"],
            row["timestamptz"],
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor = conn.cursor()
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        rows_affected = cursor.rowcount
        logging.info(f"✅ {rows_affected} rows inserted into database.")
        return rows_affected
    except sqlite3.Error as e:
        logging.error(f"❌ Error inserting data: {e}")
        conn.rollback()
        return 0


def display_data(conn: sqlite3.Connection) -> None:
    """Display recently inserted data from the database.

    Args:
        conn: SQLite connection

    """
    display_query = f"""
        SELECT currency, base_currency, exchange_rate, timestamptz
        FROM {API_TABLE_NAME}
        ORDER BY timestamptz DESC, currency ASC
        LIMIT 10;
    """

    try:
        result_df = pd.read_sql_query(display_query, conn)
        logging.info("Last 10 inserted rows:")
        print(tabulate(result_df, headers="keys", tablefmt="fancy_grid"))
    except sqlite3.Error as e:
        logging.error(f"❌ Error displaying data: {e}")


def save_to_local_db(df: pd.DataFrame) -> bool:
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


def run_api_process() -> None:
    """Main ETL pipeline function."""
    logging.info(f"⚙️ Starting ETL:API pipeline with {API_URL}")

    if not ensure_directories():
        logging.error("❌ Failed to create necessary directories. Exiting.")
        return

    raw_data = fetch_forex_data()
    if not raw_data:
        logging.error("❌ Failed to fetch data. Exiting.")
        return

    df = transform_forex_data(raw_data)

    if df is not None:
        csv_success = save_to_csv(df)
        db_success = save_to_local_db(df)
        sync_data(DB_PATH, API_TABLE_NAME, source="api")

        if csv_success and db_success:
            logging.info("✅ ETL:API process completed successfully.")
        else:
            logging.warning("⚠️ ETL:API process completed with warnings.")
    else:
        logging.error("❌ ETL:API process failed during transformation.")


if __name__ == "__main__":
    run_api_process()
