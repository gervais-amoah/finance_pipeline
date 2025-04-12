import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import pytz
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
BASE_URL = "https://www.x-rates.com/table/?from=EUR&amount=1"
TIMEZONE = pytz.timezone(
    "Europe/Berlin"
)  # CET/CEST (x-rates.com updates in local time)


def fetch_html(url=BASE_URL):
    """Fetch the HTML content from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Successfully fetched HTML content.")
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching HTML content: {e}")
        return None


def parse_exchange_rates(html):
    """Parse HTML content and extract exchange rates."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tablesorter ratesTable")

    if not table:
        logging.warning("Could not find the exchange rate table.")
        return []

    rows = table.find_all("tr")[1:]  # Skip header
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        currency_name = cols[0].get_text(strip=True)
        exchange_rate = float(cols[1].get_text(strip=True))
        data.append({"currency_name": currency_name, "exchange_rate": exchange_rate})
    logging.info(f"Parsed {len(data)} exchange rates.")
    return data


def transform_data(data, base_currency="EUR"):
    """Add metadata and format data as a DataFrame."""
    now_berlin = datetime.now(TIMEZONE)
    now_utc = now_berlin.astimezone(pytz.utc)
    date_str = now_utc.strftime("%Y-%m-%d")

    for entry in data:
        entry["base_currency"] = base_currency
        entry["date"] = date_str

    df = pd.DataFrame(data)
    logging.info(f"Transformed data into DataFrame with shape {df.shape}")
    return df


def save_to_csv(df, filename="scraped_forex_rates.csv"):
    """Save the DataFrame to a CSV file."""
    df.to_csv(filename, index=False)
    logging.info(f"Saved scraped data to {filename}")


def display_data(df, n=5):
    """Print the first few rows for inspection."""
    print(df.head(n))


def run_scraper():
    """Main entry point to run the scraper."""
    logging.info("⚙️ Starting the web scraper...")

    html = fetch_html()
    if html:
        data = parse_exchange_rates(html)
        df = transform_data(data)
        display_data(df)
        save_to_csv(df)


if __name__ == "__main__":
    run_scraper()
