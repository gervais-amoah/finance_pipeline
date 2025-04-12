from .api_fetcher import run_api_process
from .csv_loader import run_csv_loading_process
from .web_scraper import run_web_scrapping_process

__version__ = "1.0.0"

# Configurations globales, si nécessaire
from etl.config import logging


def run_etl():
    logging.info("🚀 Running ETL pipeline...")
    run_api_process()
    run_csv_loading_process()
    run_web_scrapping_process()
    logging.info("✅ ETL pipeline completed.")
