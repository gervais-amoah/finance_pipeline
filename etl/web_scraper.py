import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from tabulate import tabulate

# Constants
BASE_CURRENCY = "EUR"
URL = "https://www.x-rates.com/table/?from=EUR&amount=1"
DB_PATH = Path("database/forex_data.db")
PROCESSED_DIR = Path("data/processed/")
TABLE_NAME = "forex_rates_scraped"

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def ensure_directories() -> bool:
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logging.error(f"❌ Error creating directories: {e}")
        return False


def fetch_html() -> Optional[str]:
    try:
        logging.info("⌛ Fetching exchange rate page...")
        response = requests.get(URL)
        response.raise_for_status()
        logging.info("✅ HTML content fetched successfully.")
        return response.text
    except requests.RequestException as e:
        logging.error(f"❌ Failed to fetch page: {e}")
        return None


def extract_timestamp(html: str) -> Optional[datetime]:
    soup = BeautifulSoup(html, "html.parser")
    span = soup.find("span", class_="ratesTimestamp")
    if not span:
        logging.error("❌ Timestamp not found in page.")
        return None
    raw = span.text.strip()  # e.g. "Apr 12, 2025 18:28 UTC"
    try:
        dt = datetime.strptime(raw, "%b %d, %Y %H:%M %Z")
        return dt
    except ValueError as e:
        logging.error(f"❌ Failed to parse timestamp: {e}")
        return None


def parse_rates(html: str, timestamp: datetime) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tablesorter ratesTable")
    rows = table.find_all("tr")[1:] if table else []

    data: List[Dict] = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        currency_name = cols[0].get_text(strip=True)
        exchange_rate = float(cols[1].get_text(strip=True))
        data.append(
            {
                "currency_name": currency_name,
                "base_currency": BASE_CURRENCY,
                "exchange_rate": exchange_rate,
                "date": timestamp.date().isoformat(),
                "timestamp": timestamp.isoformat(),
            }
        )

    logging.info(f"✅ Parsed {len(data)} exchange rates.")
    return pd.DataFrame(data)


def get_csv_path(date_str: str) -> Path:
    return PROCESSED_DIR / f"forex_scraped_{date_str}.csv"


def save_to_csv(df: pd.DataFrame, date_str: str) -> bool:
    path = get_csv_path(date_str)
    try:
        if path.exists():
            existing_df = pd.read_csv(path)
            combined = pd.concat([existing_df, df]).drop_duplicates(
                subset=["currency_name", "timestamp"]
            )
        else:
            combined = df
        combined.to_csv(path, index=False)
        logging.info(f"✅ Data saved to {path}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to save to CSV: {e}")
        return False


def create_table(conn: sqlite3.Connection) -> bool:
    query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency_name TEXT,
            base_currency TEXT,
            exchange_rate REAL,
            date TEXT,
            timestamp TEXT,
            UNIQUE(currency_name, timestamp)
        );
    """
    try:
        conn.execute(query)
        conn.commit()
        logging.info(f"✅ Table `{TABLE_NAME}` is ready.")
        return True
    except sqlite3.Error as e:
        logging.error(f"❌ Error creating table: {e}")
        return False


def insert_data(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    query = f"""
        INSERT OR IGNORE INTO {TABLE_NAME}
        (currency_name, base_currency, exchange_rate, date, timestamp)
        VALUES (?, ?, ?, ?, ?)
    """
    data = [
        (
            row["currency_name"],
            row["base_currency"],
            row["exchange_rate"],
            row["date"],
            row["timestamp"],
        )
        for _, row in df.iterrows()
    ]
    try:
        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        count = cursor.rowcount
        logging.info(f"✅ {count} rows inserted into database.")
        return count
    except sqlite3.Error as e:
        logging.error(f"❌ Error inserting into DB: {e}")
        conn.rollback()
        return 0


def display_data(conn: sqlite3.Connection) -> None:
    query = f"""
        SELECT currency_name, base_currency, exchange_rate, timestamp
        FROM {TABLE_NAME}
        ORDER BY timestamp DESC
        LIMIT 10;
    """
    try:
        df = pd.read_sql_query(query, conn)
        logging.info("Last 10 inserted rows:")
        print(tabulate(df, headers="keys", tablefmt="fancy_grid"))
    except sqlite3.Error as e:
        logging.error(f"❌ Error displaying data: {e}")


def save_to_db(df: pd.DataFrame) -> bool:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            if not create_table(conn):
                return False
            inserted = insert_data(conn, df)
            if inserted > 0:
                display_data(conn)
            return True
    except sqlite3.Error as e:
        logging.error(f"❌ Database connection failed: {e}")
        return False


def run() -> None:
    logging.info("⚙️ Starting ETL:Web Scraping process...")
    if not ensure_directories():
        return

    html = fetch_html()
    if not html:
        return

    timestamp = extract_timestamp(html)
    if not timestamp:
        return

    df = parse_rates(html, timestamp)
    if df.empty:
        logging.warning("⚠️ No data extracted.")
        return

    csv_ok = save_to_csv(df, timestamp.date().isoformat())
    db_ok = save_to_db(df)

    if csv_ok and db_ok:
        logging.info("✅ ETL:Web Scraping process completed successfully.")
    else:
        logging.warning("⚠️ ETL:Web Scraping process completed with warnings.")


if __name__ == "__main__":
    run()
