import sqlite3
import pandas as pd
import os

from dotenv import load_dotenv
from supabase import create_client, Client

from etl.config import logging
from utils.email_utils import alert_admin

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_columns_except_id(conn, table_name: str) -> str:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall() if row[1] != "id"]
    return ", ".join(columns)


def upload_to_supabase(df: pd.DataFrame, source: str = ""):
    """
    Upload a pandas DataFrame to Supabase table 'forex_rates'.
    Optionally adds a source tag (e.g. 'csv', 'api', 'scraper')
    """

    if source:
        df["source"] = source

    data = df.to_dict(orient="records")

    try:
        response = supabase.table("forex_rates").insert(data).execute()

        logging.info(f"✅ [Sync] {len(df)} new rows synced to Supabase.")
    except Exception as e:
        logging.error(f"❌ [Sync] Failed to sync data to Supabase: {e}")


def sync_data(db_path: str, table_name: str, source: str):
    """
    Sync newly inserted data (last 5 minutes) from local SQLite DB to Supabase.

    Args:
        db_path: Path to the local SQLite database.
        table_name: Table to query (default: 'forex_api').
        source: Optional source tag for Supabase (default: 'api').
    """
    try:
        conn = sqlite3.connect(db_path)

        columns = get_columns_except_id(conn, table_name)

        # Query to fetch data from the last 20 minutes
        query = f"""
            SELECT {columns}
            FROM {table_name}
            WHERE created_at >= datetime('now', '-20 minutes')
        """

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            upload_to_supabase(df, source)

    except Exception as e:
        logging.error(f"❌ [Sync] Failed to sync data to Supabase: {e}")
        alert_admin(
            subject="❌ [Sync] Supabase Sync Error",
            message=f"Failed to sync data to Supabase: {e}",
        )

    finally:
        conn.close()
