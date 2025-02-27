import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from control import *
from config import *
from datetime import datetime, timedelta
from training import train
from testing import test
from push import push
import logging
from pymongo import MongoClient
from TradeSim.utils import initialize_simulation, precompute_strategy_decisions
from helper_files.client_helper import strategies, get_ndaq_tickers

import certifi
ca = certifi.where()

logs_dir = 'logs'
# Create the directory if it doesn't exist
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler(os.path.join(logs_dir, 'train_test.log'))
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

if __name__ == "__main__":

    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)

    # If no tickers provided, fetch Nasdaq tickers
    if not train_tickers:
        logger.info("No tickers provided. Fetching Nasdaq tickers...")
        train_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)
        logger.info(f"Fetched {len(train_tickers)} tickers.")

    ticker_price_history, ideal_period = initialize_simulation(
        train_period_start, test_period_end, train_tickers, mongo_client, FINANCIAL_PREP_API_KEY, logger
        )
    
    # Precompute all strategy decisions
    precomputed_decisions = precompute_strategy_decisions(
        strategies, ticker_price_history, train_tickers, ideal_period,
        train_period_start, test_period_end, logger
    )
    
    if mode == 'train':
        train(ticker_price_history, ideal_period, mongo_client, precomputed_decisions, logger)
        test(ticker_price_history, ideal_period, mongo_client, precomputed_decisions, logger)
    elif mode == 'push':
        push()
