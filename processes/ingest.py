import datetime
import logging
import os
import time
import json
import httpx
import polars as pl
from dotenv import load_dotenv
from prefect import task, flow

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

if not os.environ.get("PREFECT_API_URL"):
    os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

logging.basicConfig(level=logging.INFO)

api_key = os.environ.get("COINGECKO_API_KEY")
headers = {"x-cg-demo-api-key": api_key} if api_key else {}

@task(retries=3, retry_delay_seconds=5)
def get_coin_id(coin_name):
    search_url = "https://api.coingecko.com/api/v3/search"
    params = {"query": coin_name}
    try:
        response = httpx.get(search_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("coins") and len(data["coins"]) > 0:
            coin_id = data["coins"][0]["id"]
            logging.info(f"Found coin ID for {coin_name}: {coin_id}")
            return coin_id
        return None
    except Exception as e:
        logging.error(f"Error fetching coin ID for {coin_name}: {e}")
        raise

@task(retries=3, retry_delay_seconds=5)
def historical_data(coin_id, coin_name, date):
    if coin_id is None:
        return None
    history_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    params = {"date": date}
    try:
        response = httpx.get(history_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching historical data for {coin_name}: {e}")
        raise

@task(retries=3, retry_delay_seconds=5)
def get_market_data(coin_id):
    market_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": "30"}
    try:
        response = httpx.get(market_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching market data for coin ID {coin_id}: {e}")
        raise

@task(retries=3, retry_delay_seconds=5)
def save_data_to_parquet(all_data, output_path, schema=None):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pl.DataFrame(all_data, schema=schema)
        df.write_parquet(output_path)
        logging.info(f"Saved data to {output_path}")
    except Exception as e:
        logging.error(f"Error saving data to {output_path}: {e}")
        raise

@flow
def bronze_crypto_market_data(historical_date=None, output_path="data/bronze/crypto_market_data.parquet"):
    if historical_date is None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        historical_date = yesterday.strftime("%d-%m-%Y")

    if not os.path.isabs(output_path):
        output_path = os.path.join(PROJECT_ROOT, output_path)

    list_of_coins = ["litecoin", "ethereum", "solana"]
    all_data = []

    for coin in list_of_coins:
        # Resolve tasks via .submit() and capture value using .result()
        coin_id = get_coin_id.submit(coin).result()
        if not coin_id:
            continue

        market_future = get_market_data.submit(coin_id)
        historical_future = historical_data.submit(coin_id, coin, historical_date)

        all_data.append({
            "coin": coin,
            "coin_id": coin_id,
            "historical_date": historical_date,
            "market_data": json.dumps(market_future.result()),
            "historical_data": json.dumps(historical_future.result())
        })

        time.sleep(60)  # Safe Coingecko rate limit window

    # CRITICAL: Submit write task and explicitly block flow termination until it completes
    save_future = save_data_to_parquet.submit(all_data, output_path)
    save_future.wait() 
