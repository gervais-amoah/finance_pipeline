import pandas as pd
import sqlite3
import logging
from pathlib import Path

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration des chemins
CSV_PATH = Path("data/raw/daily_forex_rates.csv")
DB_PATH = Path("database/forex_data.db")

# Nom de la table SQL
HISTORY_TABLE_NAME = "forex_rates_history"


# 1. Chargement du CSV
def load_csv_data(path):
    logging.info(f"Chargement du fichier CSV depuis {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes chargées depuis le fichier CSV.")
    return df


# 2. Nettoyage et formatage
def clean_data(df):
    logging.info("Nettoyage des données...")
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["currency", "exchange_rate", "date"], inplace=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["exchange_rate"] > 0]
    logging.info(f"{len(df)} lignes après nettoyage.")
    return df


# 3. Création de la base de données et de la table
def create_table(conn):
    logging.info("Création de la table SQL si elle n'existe pas.")
    query = f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        currency TEXT NOT NULL,
        base_currency TEXT NOT NULL,
        currency_name TEXT,
        exchange_rate REAL NOT NULL,
        date DATE NOT NULL,
        UNIQUE(currency, date)
    );
    """
    conn.execute(query)
    conn.commit()


# 4. Insertion des données dans la base
def insert_data(conn, df):
    logging.info("Insertion des données dans la base SQL...")
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {HISTORY_TABLE_NAME}
                (currency, base_currency, currency_name, exchange_rate, date)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    row.currency,
                    row.base_currency,
                    row.currency_name,
                    row.exchange_rate,
                    row.date.strftime("%Y-%m-%d"),
                ),
            )
            inserted += conn.total_changes
        except Exception as e:
            skipped += 1
            logging.warning(f"Ligne ignorée: {e}")
    conn.commit()
    logging.info(
        f"Insertion terminée: {inserted} nouvelles lignes insérées, {skipped} lignes ignorées."
    )


# 5. Pipeline principal
def run():
    if not CSV_PATH.exists():
        logging.error(f"Fichier CSV introuvable à {CSV_PATH}")
        return

    df = load_csv_data(CSV_PATH)
    df = clean_data(df)

    with sqlite3.connect(DB_PATH) as conn:
        create_table(conn)
        insert_data(conn, df)


if __name__ == "__main__":
    run()
