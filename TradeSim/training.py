import heapq
import json
import os
import sys
import time
from datetime import datetime, timedelta

import wandb
from variables import config_dict

from config import FINANCIAL_PREP_API_KEY

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from control import (
    train_period_end,
    train_period_start,
    train_tickers,
    train_time_delta,
    train_time_delta_mode,
)

train_tickers
from helper_files.client_helper import get_ndaq_tickers, strategies
from helper_files.train_client_helper import local_update_portfolio_values
from TradeSim.utils import simulate_trading_day, update_time_delta

results_dir = "results"
if not os.path.exists(results_dir):
    os.makedirs(results_dir)


def train(
    ticker_price_history, ideal_period, mongo_client, precomputed_decisions, logger
):
    """
    get from ndaq100
    """
    global train_tickers
    if not train_tickers:
        train_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)
        logger.info(f"Fetched {len(train_tickers)} tickers.")

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
            "portfolio_value": 50000,
        }
        for strategy in strategies
    }

    points = {strategy.__name__: 0 for strategy in strategies}
    time_delta = train_time_delta

    logger.info("Trading simulator and points initialized.")

    start_date = datetime.strptime(train_period_start, "%Y-%m-%d")
    end_date = datetime.strptime(train_period_end, "%Y-%m-%d")
    current_date = start_date

    logger.info(f"Training period: {start_date} to {end_date}")
    while current_date <= end_date:
        logger.info(f"Processing date: {current_date.strftime('%Y-%m-%d')}")

        if (
            current_date.weekday() >= 5
            or current_date.strftime("%Y-%m-%d")
            not in ticker_price_history[train_tickers[0]].index
        ):
            logger.info(
                f"Skipping {current_date.strftime('%Y-%m-%d')} (weekend or missing data)."
            )
            current_date += timedelta(days=1)
            continue

        trading_simulator, points = simulate_trading_day(
            current_date,
            strategies,
            trading_simulator,
            points,
            time_delta,
            ticker_price_history,
            train_tickers,
            precomputed_decisions,
            logger,
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
        "date": current_date.strftime("%Y-%m-%d"),
        "time_delta": time_delta,
    }

    result_filename = f"{config_dict['experiment_name']}.json"
    results_file_path = os.path.join(results_dir, result_filename)
    with open(results_file_path, "w") as json_file:
        json.dump(results, json_file, indent=4)

    # Create an artifact
    artifact = wandb.Artifact(result_filename, type="results")
    artifact.add_file(results_file_path)

    # Log artifact to the current run
    wandb.log_artifact(artifact)

    logger.info(f"Training results saved to {results_file_path}")

    top_portfolio_values = heapq.nlargest(
        10, trading_simulator.items(), key=lambda x: x[1]["portfolio_value"]
    )
    top_points = heapq.nlargest(10, points.items(), key=lambda x: x[1])

    top_portfolio_values_list = []
    logger.info("Top 10 strategies with highest portfolio values:")
    for strategy, value in top_portfolio_values:
        top_portfolio_values_list.append([strategy, value["portfolio_value"]])
        logger.info(f"{strategy} - {value['portfolio_value']}")

    wandb.log({"TRAIN_top_portfolio_values": top_portfolio_values_list})
    wandb.log({"TRAIN_top_points": top_points})

    logger.info("Top 10 strategies with highest points:")
    for strategy, value in top_points:
        logger.info(f"{strategy} - {value}")

    logger.info("Training completed.")
