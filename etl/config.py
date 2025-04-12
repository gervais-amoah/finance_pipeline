import logging
import pytz

from pathlib import Path
from datetime import datetime, time

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants
DEFAULT_CURRENCY = "EUR"

RAW_CSV_FILE_URL = "https://www.kaggle.com/datasets/asaniczka/forex-exchange-rate-since-2004-updated-daily/versions/529/data"
WEBPAGE_URL = f"https://www.x-rates.com/table/?from={DEFAULT_CURRENCY}&amount=1"
API_URL = "https://api.frankfurter.app/latest"

DB_PATH = Path("database/forex_data.db")
PROCESSED_FILES_PATH = Path("data/processed")

API_TABLE_NAME = "forex_rates_api"
HISTORY_TABLE_NAME = "forex_rates_history"
WEB_SCRAPPER_TABLE_NAME = "forex_rates_scraped"

RAW_CSV_FILE_PATH = Path("data/raw/daily_forex_rates.csv")
CSV_FILE_PATH = Path("data/processed/forex_api.csv")


API_UPDATE_TIME = time(hour=16, minute=0)
CET_TIMEZONE = pytz.timezone("CET")
