

from TradeSim.utils import initialize_simulation, simulate_trading_day, update_time_delta
from config import *
from utils import * 
import heapq
import certifi
from pymongo import MongoClient
from control import *
import os
import logging
from helper_files.client_helper import *
from helper_files.train_client_helper import *
from datetime import datetime, timedelta
from ranking_client import update_ranks

ca = certifi.where()

results_dir = 'results'
logs_dir = 'logs'

# Create the directory if it doesn't exist
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

if not os.path.exists(results_dir):
        os.makedirs(results_dir)   

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler(os.path.join(logs_dir, 'training.log'))
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

def push():
    with open('training_results.json', 'r') as json_file:
        results = json.load(json_file)
        trading_simulator = results['trading_simulator']
        points = results['points']
        date = results['date']
        time_delta = results['time_delta']

    # Push the trading simulator and points to the database
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    db = mongo_client.trading_simulator
    holdings_collection = db.algorithm_holdings
    points_collection = db.points_tally

    for strategy, value in trading_simulator.items():
        holdings_collection.update_one(
            {"strategy": strategy},
            {
                "$set": {
                    "holdings": value["holdings"],
                    "amount_cash": value["amount_cash"],
                    "total_trades": value["total_trades"],
                    "successful_trades": value["successful_trades"],
                    "neutral_trades": value["neutral_trades"],
                    "failed_trades": value["failed_trades"],
                    "portfolio_value": value["portfolio_value"],
                    "last_updated": datetime.now(),
                    "initialized_date": datetime.now()
                }
            },
            upsert=True
        )

    for strategy, value in points.items():
        points_collection.update_one(
            {"strategy": strategy},
            {
                "$set": {
                    "total_points": value,
                    "last_updated": datetime.now(),
                    "initialized_date": datetime.now()
                }
            },
            upsert=True
        )

    db.time_delta.update_one({}, {"$set": {"time_delta": time_delta}}, upsert=True)
    update_ranks(mongo_client)