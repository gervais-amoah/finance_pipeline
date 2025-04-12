import pandas as pd
import sqlite3
import logging
from pathlib import Path
from tabulate import tabulate

# Logger configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Path configuration
CSV_PATH = Path("data/processed/forex_rates_4m.csv")
DB_PATH = Path("database/forex_data.db")

# SQL table name
HISTORY_TABLE_NAME = "forex_rates_history"


def load_csv_data(path: Path) -> pd.DataFrame:
    """Load data from CSV file.

    Args:
        path: Path to the CSV file

    Returns:
        DataFrame containing the CSV data
    """
    logging.info(f"Loading CSV file from {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} rows loaded from CSV file.")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and format DataFrame.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    logging.info("Cleaning data...")
    # Create a copy to avoid SettingWithCopyWarning
    cleaned_df = df.copy()

    cleaned_df.drop_duplicates(inplace=True)
    cleaned_df.dropna(subset=["currency", "exchange_rate", "date"], inplace=True)
    cleaned_df["date"] = pd.to_datetime(cleaned_df["date"], errors="coerce")
    cleaned_df = cleaned_df[cleaned_df["exchange_rate"] > 0]

    # Add timestamp_utc based on 10:00:00 UTC
    cleaned_df["timestamp_utc"] = cleaned_df["date"] + pd.Timedelta(hours=10)
    logging.info(f"{len(cleaned_df)} rows after cleaning.")
    return cleaned_df


def create_table(conn: sqlite3.Connection) -> None:
    """Create SQL table if it doesn't exist.

    Args:
        conn: SQLite connection
    """
    logging.info("Creating SQL table if it doesn't exist.")
    query = f"""
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
    conn.execute(query)
    conn.commit()


def insert_data(conn: sqlite3.Connection, df: pd.DataFrame) -> tuple:
    """Insert data into SQLite database.

    Args:
        conn: SQLite connection
        df: DataFrame containing data to insert

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    logging.info("Inserting data into SQL database...")
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
        logging.error(f"Batch insert failed: {e}")
        logging.info("Falling back to row-by-row insertion...")

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
                logging.warning(f"Row skipped: {e}")

    conn.commit()
    logging.info(
        f"✅ Insertion completed: {inserted} new rows inserted, {skipped} rows skipped."
    )
    return inserted, skipped


def display_data(conn: sqlite3.Connection) -> None:
    """Display sample of the inserted data.

    Args:
        conn: SQLite connection
    """
    logging.info("Displaying inserted data (limited to 10 rows):")
    query = f"""
        SELECT currency, base_currency, exchange_rate, timestamp_utc
        FROM {HISTORY_TABLE_NAME}
        ORDER BY timestamp_utc DESC, currency ASC
        LIMIT 10;
    """
    df = pd.read_sql_query(query, conn)
    print(tabulate(df, headers="keys", tablefmt="fancy_grid"))


def ensure_directories() -> bool:
    """Ensure required directories exist.

    Returns:
        Boolean indicating if setup was successful
    """
    # Ensure the database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Check if CSV file exists
    if not CSV_PATH.exists():
        logging.error(f"❌ CSV file not found at {CSV_PATH}")
        return False
    return True


def run() -> None:
    """Main function to orchestrate the data processing pipeline."""
    if not ensure_directories():
        return

    df = load_csv_data(CSV_PATH)
    df = clean_data(df)

    with sqlite3.connect(DB_PATH) as conn:
        create_table(conn)
        insert_data(conn, df)
        display_data(conn)


if __name__ == "__main__":
    run()
