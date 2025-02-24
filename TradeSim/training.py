from config import *
from utils import * 
import heapq
import certifi
from pymongo import MongoClient

import os
import logging

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

# def push():
#     with open('training_results.json', 'r') as json_file:
#          results = json.load(json_file)
#          trading_simulator = results['trading_simulator']
#          points = results['points']
#          date = results['date']
#          time_delta = results['time_delta']
#     # Push the trading simulator and points to the database
#     mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
#     db = mongo_client.trading_simulator
#     holdings_collection = db.algorithm_holdings
#     points_collection = db.points_tally
#     for strategy, value in trading_simulator.items():
#         holdings_collection.update_one(
#             {"strategy": strategy},
#             {
#                 "$set": {
#                     "holdings": value["holdings"],
#                     "amount_cash": value["amount_cash"],
#                     "total_trades": value["total_trades"],
#                     "successful_trades": value["successful_trades"],
#                     "neutral_trades": value["neutral_trades"],
#                     "failed_trades": value["failed_trades"],
#                     "portfolio_value": value["portfolio_value"],
#                     "last_updated": datetime.now(),
#                     "initialized_date": datetime.now()
#                 }
#             },
#             upsert=True
#         )
#     for strategy, value in points.items():
#         points_collection.update_one(
#             {"strategy": strategy},
#             {
#                 "$set": {
#                     "total_points": value,
#                     "last_updated": datetime.now(),
#                     "initialized_date": datetime.now()
#                 }
#             },
#             upsert=True
#         )
#     db.time_delta.update_one({}, {"$set": {"time_delta": time_delta}}, upsert=True)
#     update_ranks(mongo_client)


def train():
    logger.info("Initializing simulation...")
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    ticker_price_history, ideal_period = initialize_simulation(
        period_start, period_end, train_tickers, mongo_client, FINANCIAL_PREP_API_KEY, logger
    )
    
    logger.info(f"Ticker price history initialized for {len(train_tickers)} tickers.")
    # logger.info(f"Ideal period determined: {ideal_period}")
    
    trading_simulator = {
        strategy.__name__: {
            "holdings": {},
            "amount_cash": 50000,
            "total_trades": 0,
            "successful_trades": 0,
            "neutral_trades": 0,
            "failed_trades": 0,
            "portfolio_value": 50000
        } for strategy in strategies
    }
    
    points = {strategy.__name__: 0 for strategy in strategies}
    time_delta = train_time_delta
    
    logger.info("Trading simulator and points initialized.")

    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    end_date = datetime.strptime(period_end, "%Y-%m-%d")
    current_date = start_date
    
    logger.info(f"Training period: {start_date} to {end_date}")
    while current_date <= end_date:
        logger.info(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
        
        if current_date.weekday() >= 5 or current_date.strftime('%Y-%m-%d') not in ticker_price_history[train_tickers[0]].index:
            logger.info(f"Skipping {current_date.strftime('%Y-%m-%d')} (weekend or missing data).")
            current_date += timedelta(days=1)
            continue
            
        trading_simulator, points = simulate_trading_day(
            current_date, strategies, trading_simulator, points, 
            time_delta, ticker_price_history, train_tickers, ideal_period, logger
        )

        active_count, trading_simulator = local_update_portfolio_values(
            current_date, strategies, trading_simulator, ticker_price_history, logger
        )

        logger.info(f"Trading simulator: {trading_simulator}")
        logger.info(f"Points: {points}")
        logger.info(f"Date: {current_date.strftime('%Y-%m-%d')}")
        logger.info(f"time_delta: {time_delta}")
        logger.info(f"Active count: {active_count}")
        logger.info("-------------------------------------------------")
        
        # Update time delta
        time_delta = update_time_delta(time_delta, train_time_delta_mode)
        logger.info(f"Updated time delta: {time_delta}")

        # Move to next day
        current_date += timedelta(days=1)
        time.sleep(5)

    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
        logger.info(f"Created results directory: {results_dir}")

    results = {
        "trading_simulator": trading_simulator,
        "points": points,
        "date": current_date.strftime('%Y-%m-%d'),
        "time_delta": time_delta
    }

    results_file_path = os.path.join(results_dir, 'training_results.json')
    with open(results_file_path, 'w') as json_file:
        json.dump(results, json_file, indent=4)
    
    logger.info(f"Training results saved to {results_file_path}")

    top_portfolio_values = heapq.nlargest(10, trading_simulator.items(), key=lambda x: x[1]["portfolio_value"])
    top_points = heapq.nlargest(10, points.items(), key=lambda x: x[1])

    logger.info("Top 10 strategies with highest portfolio values:")
    for strategy, value in top_portfolio_values:
        logger.info(f"{strategy} - {value['portfolio_value']}")
    
    logger.info("Top 10 strategies with highest points:")
    for strategy, value in top_points:
        logger.info(f"{strategy} - {value}")

    logger.info("Training completed.")


