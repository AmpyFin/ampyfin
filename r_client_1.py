'''
MODIFICATIONS: 
- Refactor code to improve readability and maintainability

TODO: 
- # The current price can be gotten through a cache system maybe
            # if polygon api is getting clogged - but that hasn't happened yet
            # Also implement in C++ or C instead of python
            # Get the current price of the ticker from the Polygon API
            # Use a cache system to store the latest prices
            # If the cache is empty, fetch the latest price from the Polygon API
            # Cache should be updated every 60 seconds
'''

import heapq
import logging
import time
from datetime import datetime

import certifi
from pymongo import MongoClient

from config import mongo_url
from control import (
    loss_price_change_ratio_d1,
    loss_price_change_ratio_d2,
    loss_profit_time_d1,
    loss_profit_time_d2,
    loss_profit_time_else,
    profit_price_change_ratio_d1,
    profit_price_change_ratio_d2,
    profit_profit_time_d1,
    profit_profit_time_d2,
    profit_profit_time_else,
    rank_asset_limit,
    rank_liquidity_limit,
    time_delta_balanced,
    time_delta_increment,
    time_delta_mode,
    time_delta_multiplicative,
)
from helper_files.client_helper import get_latest_price, get_ndaq_tickers, strategies
from strategies.talib_indicators import get_data, simulate_strategy

# TLS certificate
ca = certifi.where()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("rank_system.log"),
        logging.StreamHandler(),
    ],
)


def get_mongo_client():
    """
    Create a MongoDB client with TLS certificate.
    """
    return MongoClient(mongo_url, tlsCAFile=ca)


def points_update_func(ticker, current_price, strategy, strategy_doc, holdings_doc, time_delta, 
                       sell_qty, holdings_collection, points_collection):
    """
    Calculate and update points based on trade result
    """
    price_change_ratio = (
        current_price / holdings_doc[ticker]["price"]
        if ticker in holdings_doc
        else 1
    )

    if current_price > holdings_doc[ticker]["price"]:
        # increment successful trades
        holdings_collection.update_one(
            {"strategy": strategy.__name__},
            {"$inc": {"successful_trades": 1}},
            upsert=True,
        )

        # Calculate points to add if the current price is higher than the purchase price
        if price_change_ratio < profit_price_change_ratio_d1:
            points = time_delta * profit_profit_time_d1
        elif price_change_ratio < profit_price_change_ratio_d2:
            points = time_delta * profit_profit_time_d2
        else:
            points = time_delta * profit_profit_time_else

    else:
        # Calculate points to deduct if the current price is lower than the purchase price
        if holdings_doc[ticker]["price"] == current_price:
            holdings_collection.update_one(
                {"strategy": strategy.__name__}, {"$inc": {"neutral_trades": 1}}
            )

        else:
            holdings_collection.update_one(
                {"strategy": strategy.__name__},
                {"$inc": {"failed_trades": 1}},
                upsert=True,
            )

        if price_change_ratio > loss_price_change_ratio_d1:
            points = -time_delta * loss_profit_time_d1
        elif price_change_ratio > loss_price_change_ratio_d2:
            points = -time_delta * loss_profit_time_d2
        else:
            points = -time_delta * loss_profit_time_else

    # Update the points tally
    points_collection.update_one(
        {"strategy": strategy.__name__},
        {
            "$set": {"last_updated": datetime.now()},
            "$inc": {"total_points": points},
        },
        upsert=True,
    )
    
    # Update cash after selling
    holdings_collection.update_one(
        {"strategy": strategy.__name__},
        {
            "$set": {
                "holdings": holdings_doc,
                "amount_cash": strategy_doc["amount_cash"] + sell_qty * current_price,
                "last_updated": datetime.now(),
            },
            "$inc": {"total_trades": 1},
        },
        upsert=True,
    )


def sell_logic(ticker, action, quantity, current_price, strategy, strategy_doc, 
               holdings_doc, time_delta, holdings_collection, points_collection):
    """
    Handle sell order logic
    """
    logging.info(
        f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}"
    )
    current_qty = holdings_doc[ticker]["quantity"]

    # Ensure we do not sell more than we have
    sell_qty = min(quantity, current_qty)
    holdings_doc[ticker]["quantity"] = current_qty - sell_qty

    points_update_func(ticker, current_price, strategy, strategy_doc, holdings_doc, 
                      time_delta, sell_qty, holdings_collection, points_collection)
                      
    # Remove the ticker if quantity reaches zero
    if holdings_doc[ticker]["quantity"] == 0:
        del holdings_doc[ticker]


def buy_logic(ticker, quantity, current_price, strategy, strategy_doc, holdings_doc, holdings_collection):
    """
    Handle buy order logic
    """
    logging.info(
        f"Action: buy | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}"
    )
    
    # Calculate average price if already holding some shares of the ticker
    if ticker in holdings_doc:
        current_qty = holdings_doc[ticker]["quantity"]
        new_qty = current_qty + quantity
        average_price = (
            holdings_doc[ticker]["price"] * current_qty + current_price * quantity
        ) / new_qty
    else:
        new_qty = quantity
        average_price = current_price

    # Update the holdings document for the ticker.
    holdings_doc[ticker] = {"quantity": new_qty, "price": average_price}

    # Deduct the cash used for buying and increment total trades
    holdings_collection.update_one(
        {"strategy": strategy.__name__},
        {
            "$set": {
                "holdings": holdings_doc,
                "amount_cash": strategy_doc["amount_cash"] - quantity * current_price,
                "last_updated": datetime.now(),
            },
            "$inc": {"total_trades": 1},
        },
        upsert=True,
    )


def process_ticker(ticker, client):
    """
    Process a single ticker across all strategies.
    """
    try:
        current_price = get_latest_price(ticker)

        indicator_coll = client.IndicatorsDatabase.Indicators
        for strategy in strategies:
            try:
                period = indicator_coll.find_one({"indicator": strategy.__name__})
                if not period:
                    logging.warning(f"No period found for indicator {strategy.__name__}")
                    continue
                
                historical_data = None
                retry_count = 0
                while historical_data is None and retry_count < 3:
                    try:
                        historical_data = get_data(ticker, client, period["ideal_period"])
                    except Exception as fetch_error:
                        retry_count += 1
                        logging.warning(f"Error fetching data for {ticker}: {fetch_error}. Retry {retry_count}/3")
                        time.sleep(10)
                
                if historical_data is None:
                    logging.error(f"Failed to fetch historical data for {ticker} after 3 attempts")
                    continue

                holdings_coll = client.trading_simulator.algorithm_holdings
                strategy_doc = holdings_coll.find_one({"strategy": strategy.__name__})
                if not strategy_doc:
                    logging.warning(f"Skipping missing strategy doc: {strategy.__name__}")
                    continue

                simulate_trade(
                    ticker,
                    strategy,
                    historical_data,
                    current_price,
                    strategy_doc,
                    client,
                )
            except Exception as strat_error:
                logging.error(f"Error processing strategy {strategy.__name__} for {ticker}: {strat_error}")

        logging.info(f"Completed processing ticker {ticker}")
    except Exception as e:
        logging.error(f"Error in process_ticker({ticker}): {e}")


def simulate_trade(
    ticker,
    strategy,
    historical_data,
    current_price,
    strat_doc,
    client,
):
    """
    Simulate a trade for one strategy and update MongoDB.
    """
    portfolio_qty = strat_doc.get("holdings", {}).get(ticker, {}).get("quantity", 0)
    
    action, quantity = simulate_strategy(
        strategy,
        ticker,
        current_price,
        historical_data,
        strat_doc["amount_cash"],
        portfolio_qty,
        strat_doc["portfolio_value"],
    )

    holdings_coll = client.trading_simulator.algorithm_holdings
    points_coll = client.trading_simulator.points_tally
    time_delta = client.trading_simulator.time_delta.find_one({})["time_delta"]
    holdings = strat_doc.get("holdings", {})

    # BUY logic
    if (
        action == "buy" 
        and strat_doc["amount_cash"] - quantity * current_price > rank_liquidity_limit
        and quantity > 0 
        and ((portfolio_qty + quantity) * current_price) / strat_doc["portfolio_value"] < rank_asset_limit
    ):
        buy_logic(ticker, quantity, current_price, strategy, strat_doc, holdings, holdings_coll)
    # SELL logic
    elif (
        action == "sell"
        and ticker in holdings
        and holdings[ticker]["quantity"] > 0
    ):
        sell_logic(ticker, action, quantity, current_price, strategy, strat_doc, 
                  holdings, time_delta, holdings_coll, points_coll)
    else:
        logging.info(f"Holding {ticker} for {strategy.__name__}")


def update_portfolio_values(client):
    """
    Update each strategy document's portfolio value based on current prices.
    """
    coll = client.trading_simulator.algorithm_holdings
    for doc in coll.find():
        value = doc["amount_cash"]
        for tkr, data in doc.get("holdings", {}).items():
            try:
                price = get_latest_price(tkr)
                if price is None:
                    logging.warning(f"Could not get price for {tkr}, using estimated value")
                    price = data["price"]  # Use last known price as fallback
                value += price * data["quantity"]
            except Exception as e:
                logging.error(f"Error updating portfolio value for {tkr}: {e}")
                # Use last known price as fallback
                value += data["price"] * data["quantity"]
                
        coll.update_one(
            {"strategy": doc["strategy"]},
            {"$set": {"portfolio_value": value}},
        )


def update_ranks(client):
    """
    Rank strategies based on points, performance, and portfolio value.
    """
    pts_coll = client.trading_simulator.points_tally
    rank_coll = client.trading_simulator.rank
    holdings_coll = client.trading_simulator.algorithm_holdings
    
    # Clear existing ranks
    rank_coll.delete_many({})

    # Clear historical database
    client.HistoricalDatabase.HistoricalDatabase.delete_many({})
    
    heap = []
    for doc in holdings_coll.find():
        strategy_name = doc["strategy"]
        
        # Skip test strategies
        if strategy_name in ["test", "test_strategy"]:
            continue
            
        pts_doc = pts_coll.find_one({"strategy": strategy_name})
        if not pts_doc:
            logging.warning(f"No points document for strategy {strategy_name}")
            total_points = 0
        else:
            total_points = pts_doc["total_points"]
        
        performance_diff = doc.get("successful_trades", 0) - doc.get("failed_trades", 0)
        
        if total_points > 0:
            # Good performing strategies: use total_points*2 + portfolio_value as score
            score = (total_points * 2 + doc["portfolio_value"], 
                    performance_diff,
                    doc["amount_cash"],
                    strategy_name)
        else:
            # Poor performing strategies: use portfolio_value as score
            score = (doc["portfolio_value"],
                    performance_diff,
                    doc["amount_cash"],
                    strategy_name)
                    
        heapq.heappush(heap, score)

    rank = 1
    while heap:
        _, _, _, strategy = heapq.heappop(heap)
        rank_coll.insert_one({"strategy": strategy, "rank": rank})
        rank += 1
        
    logging.info("Successfully updated ranks")
    logging.info("Successfully cleared historical database")


def process_market_open(client):
    """
    Market open phase: update ranks, process tickers, execute buys.
    """
    logging.info("Market OPEN: Updating ranks and processing tickers.")
    update_ranks(client)

    tickers = get_ndaq_tickers()
    for ticker in tickers:
        process_ticker(ticker, client)
    
    logging.info("Finished processing all tickers. Waiting for 30 seconds.")
    time.sleep(30)


def process_early_hours(client, early_hour_first_iteration):
    """
    Market early hours phase: prep work.
    """
    if early_hour_first_iteration:
        logging.info("Market EARLY_HOURS: Refreshing ticker list.")
        get_ndaq_tickers()  # Refresh tickers
        early_hour_first_iteration = False
        
    logging.info("Market EARLY_HOURS: Waiting 30s.")
    time.sleep(30)
    return early_hour_first_iteration, True  # Return updated flag and set post_market flag True


def process_market_closed(client, post_market_hour_first_iteration):
    """
    Market closed phase: post-market updates.
    """
    if post_market_hour_first_iteration:
        logging.info("Market CLOSED: Performing post-market operations.")
        
        # Update time delta based on the mode
        td_coll = client.trading_simulator.time_delta
        td_doc = td_coll.find_one({})
        
        if not td_doc:
            logging.error("No time_delta document found!")
            td_coll.insert_one({"time_delta": 1.0})  # Create default
        else:
            current_td = td_doc["time_delta"]
            
            if time_delta_mode == "additive":
                td_coll.update_one({}, {"$inc": {"time_delta": time_delta_increment}})
            elif time_delta_mode == "multiplicative":
                td_coll.update_one({}, {"$mul": {"time_delta": time_delta_multiplicative}})
            else:  # balanced mode
                td_coll.update_one({}, {"$inc": {"time_delta": current_td * time_delta_balanced}})

        update_portfolio_values(client)
        update_ranks(client)
        post_market_hour_first_iteration = False
        
    time.sleep(60)
    return post_market_hour_first_iteration, True  # Return updated flag and set early_hour flag True


def main():
    """
    Main control loop: dispatch based on market status.
    """
    early_hour_first_iteration = True
    post_market_hour_first_iteration = True
    
    while True:
        try:
            client = get_mongo_client()
            market_status_doc = client.market_data.market_status.find_one({})
            
            if not market_status_doc:
                logging.error("No market status document found!")
                time.sleep(60)
                continue
                
            status = market_status_doc["market_status"]
            
            if status == "open":
                process_market_open(client)
                # Reset flags when market closes
                early_hour_first_iteration = True
                post_market_hour_first_iteration = True
                
            elif status == "early_hours":
                early_hour_first_iteration, post_market_hour_first_iteration = process_early_hours(
                    client, early_hour_first_iteration
                )
                
            elif status == "closed":
                post_market_hour_first_iteration, early_hour_first_iteration = process_market_closed(
                    client, post_market_hour_first_iteration
                )
                
            else:
                logging.error(f"Unknown market status: {status}")
                time.sleep(60)
                
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(60)
        finally:
            client.close()


if __name__ == "__main__":
    main()