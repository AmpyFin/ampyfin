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
from TradeSim.utils import initialize_simulation

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
    ticker_price_history, ideal_period = initialize_simulation(
        train_period_start, test_period_end, train_tickers, mongo_client, FINANCIAL_PREP_API_KEY, logger
        )
    
    if mode == 'train':
        train(ticker_price_history, ideal_period, mongo_client, logger)
        test(ticker_price_history, ideal_period, mongo_client, logger)
    elif mode == 'push':
        push()
