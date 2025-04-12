import requests
import pandas as pd
import sqlite3
from pathlib import Path
import logging
import pytz
from datetime import datetime, time
from tabulate import tabulate

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constantes
DB_PATH = Path("database/forex_data.db")
API_TABLE_NAME = "forex_rates_api"
API_URL = "https://api.frankfurter.app/latest"

# Updated daily around 16:00 CET.
API_UPDATING_HOUR = 16
API_UPDATING_MINUTES = 0

BASE_CURRENCY = "EUR"
csv_file_path = "data/processed/forex_api.csv"


# Fonction pour charger les données depuis l'API Frankfurter
def fetch_forex_data():
    params = {"base": BASE_CURRENCY}
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        logging.info("Données récupérées avec succès.")
    except requests.RequestException as e:
        logging.error(f"Erreur de récupération des données depuis l'API: {e}")
        return None

    try:
        data = response.json()
        rates = data.get("rates", {})
        date = data.get("date", str(datetime.now().date()))

        # Construire une datetime CET à 16:00
        date_str = data.get("date", str(datetime.now().date()))
        cet = pytz.timezone("CET")
        cet_dt = cet.localize(
            datetime.combine(
                datetime.strptime(date_str, "%Y-%m-%d"),
                time(API_UPDATING_HOUR, API_UPDATING_MINUTES),
            )
        )

        # Convertir en UTC
        utc_dt = cet_dt.astimezone(pytz.utc)
        utc_timestamp = utc_dt.isoformat()

        df = pd.DataFrame(rates.items(), columns=["currency", "exchange_rate"])
        df["base_currency"] = BASE_CURRENCY
        df["date"] = date
        df["timestamp_utc"] = utc_timestamp

        logging.info("Données JSON converties en DataFrame avec succès.")
        logging.info(f"Nombre de lignes récupérées: {len(df)}")

        return df
    except ValueError as e:
        logging.error(f"Erreur de conversion des données JSON: {e}")
        return None


# Fonction pour sauvegarder les données dans le fichier CSV
def save_to_csv(df):
    try:
        if Path(csv_file_path).exists():
            df.to_csv(csv_file_path, mode="a", header=False, index=False)
            logging.info(f"Données ajoutées à {csv_file_path}")
        else:
            Path(csv_file_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_file_path, mode="w", header=True, index=False)
            logging.info(f"Fichier {csv_file_path} créé avec les données.")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde CSV: {e}")


# Fonction pour sauvegarder dans la base de données
def save_to_database(df):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        query = f"""
            CREATE TABLE IF NOT EXISTS {API_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                currency TEXT,
                base_currency TEXT,
                exchange_rate REAL,
                date TEXT,
                timestamp_utc TEXT,
                UNIQUE(currency, date)
            )
        """

        cursor.execute(query)

        for _, row in df.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {API_TABLE_NAME} (currency, base_currency, exchange_rate, date, timestamp_utc)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    row["currency"],
                    row["base_currency"],
                    row["exchange_rate"],
                    row["date"],
                    row["timestamp_utc"],
                ),
            )

        conn.commit()

        # Afficher les données insérées (limité à 10 lignes pour la lisibilité)
        cursor.execute(
            f"SELECT * FROM {API_TABLE_NAME} ORDER BY date DESC, currency ASC LIMIT 10"
        )
        rows = cursor.fetchall()

        # Lire les noms des colonnes
        column_names = [description[0] for description in cursor.description]

        # Affichage propre
        print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))

        conn.close()
        logging.info("Données ajoutées à la base de données.")
    except sqlite3.Error as e:
        logging.error(f"Erreur lors de l'insertion dans la base de données: {e}")


# Fonction principale
def run():
    logging.info(f"Démarrage du pipeline ETL avec l'API {API_URL}")
    df = fetch_forex_data()
    if df is not None:
        save_to_csv(df)
        save_to_database(df)


if __name__ == "__main__":
    run()
