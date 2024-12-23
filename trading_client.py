from polygon import RESTClient
from config import POLYGON_API_KEY, FINANCIAL_PREP_API_KEY, MONGO_DB_USER, MONGO_DB_PASS, API_KEY, API_SECRET, BASE_URL, mongo_url
import json
import certifi
from urllib.request import urlopen
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
from helper_files.client_helper import place_order, get_ndaq_tickers, market_status, strategies, get_latest_price, dynamic_period_selector
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.archived_strategies.trading_strategies_v1 import get_historical_data
import yfinance as yf
import logging
from collections import Counter
from statistics import median, mode
import statistics
import heapq
import requests
from strategies.talib_indicators import *


# MongoDB connection string

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('system.log'),  # Log messages to a file
        logging.StreamHandler()             # Log messages to the console
    ]
)


def weighted_majority_decision_and_median_quantity(decisions_and_quantities):  
    """  
    Determines the majority decision (buy, sell, or hold) and returns the weighted median quantity for the chosen action.  
    Groups 'strong buy' with 'buy' and 'strong sell' with 'sell'.
    Applies weights to quantities based on strategy coefficients.  
    """  
    buy_decisions = ['buy', 'strong buy']  
    sell_decisions = ['sell', 'strong sell']  

    weighted_buy_quantities = []
    weighted_sell_quantities = []
    buy_weight = 0
    sell_weight = 0
    hold_weight = 0
    
    # Process decisions with weights
    for decision, quantity, weight in decisions_and_quantities:
        if decision in buy_decisions:
            weighted_buy_quantities.extend([quantity])
            buy_weight += weight
        elif decision in sell_decisions:
            weighted_sell_quantities.extend([quantity])
            sell_weight += weight
        elif decision == 'hold':
            hold_weight += weight
    
    
    # Determine the majority decision based on the highest accumulated weight
    if buy_weight > sell_weight and buy_weight > hold_weight:
        return 'buy', median(weighted_buy_quantities) if weighted_buy_quantities else 0, buy_weight, sell_weight, hold_weight
    elif sell_weight > buy_weight and sell_weight > hold_weight:
        return 'sell', median(weighted_sell_quantities) if weighted_sell_quantities else 0, buy_weight, sell_weight, hold_weight
    else:
        return 'hold', 0, buy_weight, sell_weight, hold_weight

def main():
    """
    Main function to control the workflow based on the market's status.
    """
    ndaq_tickers = []
    early_hour_first_iteration = True
    post_hour_first_iteration = True
    client = RESTClient(api_key=POLYGON_API_KEY)
    trading_client = TradingClient(API_KEY, API_SECRET)
    data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
    mongo_client = MongoClient(mongo_url)
    db = mongo_client.trades
    asset_collection = db.assets_quantities
    strategy_to_coefficient = {}
    while True:
        
        client = RESTClient(api_key=POLYGON_API_KEY)
        trading_client = TradingClient(API_KEY, API_SECRET)
        data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
        status = market_status(client)  # Use the helper function for market status
        db = mongo_client.trades
        asset_collection = db.assets_quantities
        market_db = mongo_client.market_data
        market_collection = market_db.market_status
        indicator_tb = mongo_client.IndicatorsDatabase
        indicator_collection = indicator_tb.Indicators
        
        market_collection.update_one({}, {"$set": {"market_status": status}})
        
        if status == "open":
            
            if not ndaq_tickers:
                logging.info("Market is open. Waiting for 60 seconds.")
                ndaq_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)  # Fetch tickers using the helper function
                sim_db = mongo_client.trading_simulator
                rank_collection = sim_db.rank
                r_t_c_collection = sim_db.rank_to_coefficient
                for strategy in strategies:
                    rank = rank_collection.find_one({'strategy': strategy.__name__})['rank']
                    coefficient = r_t_c_collection.find_one({'rank': rank})['coefficient']
                    strategy_to_coefficient[strategy.__name__] = coefficient
                    early_hour_first_iteration = False
                    post_hour_first_iteration = True
            account = trading_client.get_account()
            qqq_latest = get_latest_price('QQQ')
            spy_latest = get_latest_price('SPY')
            
            buy_heap = []
            suggestion_heap = []
            """
            suggestion heap will be given secondary priority but it is to encourage hte program to be less pragmatic - it will buy when it can
            """
            for ticker in ndaq_tickers:
                decisions_and_quantities = []
                try:
                    trading_client = TradingClient(API_KEY, API_SECRET)
                    
                    account = trading_client.get_account()

                    buying_power = float(account.cash)
                    portfolio_value = float(account.portfolio_value)
                    cash_to_portfolio_ratio = buying_power / portfolio_value
                    
                    trades_db = mongo_client.trades
                    portfolio_collection = trades_db.portfolio_values
                    
                    """
                    we update instead of insert
                    """
                    
                    portfolio_collection.update_one({"name" : "portfolio_percentage"}, {"$set": {"portfolio_value": (portfolio_value-50000)/50000}})
                    portfolio_collection.update_one({"name" : "ndaq_percentage"}, {"$set": {"portfolio_value": (qqq_latest-503.17)/503.17}})
                    portfolio_collection.update_one({"name" : "spy_percentage"}, {"$set": {"portfolio_value": (spy_latest-590.50)/590.50}})
                    
                    
                    current_price = None
                    while current_price is None:
                        try:
                            current_price = get_latest_price(ticker)
                        except:
                            print(f"Error fetching price for {ticker}. Retrying...")
                            time.sleep(10)
                    print(f"Current price of {ticker}: {current_price}")

                    asset_info = asset_collection.find_one({'symbol': ticker})
                    portfolio_qty = asset_info['quantity'] if asset_info else 0.0
                    print(f"Portfolio quantity for {ticker}: {portfolio_qty}")
                    """
                    use weight from each strategy to determine how much each decision will be weighed. weights will be in decimal
                    """
                    for strategy in strategies:
                        historical_data = None
                        while historical_data is None:
                            try:
                                
                                
                                period = indicator_collection.find_one({'indicator': strategy.__name__})
                                
                                historical_data = get_data(ticker, mongo_client, period['ideal_period'])
                            except:
                                print(f"Error fetching data for {ticker}. Retrying...")
                        
                        decision, quantity = simulate_strategy(strategy, ticker, current_price, historical_data,
                                                      buying_power, portfolio_qty, portfolio_value)
                        weight = strategy_to_coefficient[strategy.__name__]
                        
                        decisions_and_quantities.append((decision, quantity, weight))
                        
                    decision, quantity, buy_weight, sell_weight, hold_weight = weighted_majority_decision_and_median_quantity(decisions_and_quantities)
                    
                    if portfolio_qty == 0.0 and buy_weight > sell_weight and (((quantity + portfolio_qty) * current_price) / portfolio_value) < 0.1:
                        print(f"Suggestions for buying for {ticker} with a weight of {buy_weight}")
                        max_investment = portfolio_value * 0.10
                        buy_quantity = min(int(max_investment // current_price), int(buying_power // current_price))
                        

                        heapq.heappush(suggestion_heap, (-buy_weight, buy_quantity, ticker))
                    print(f"Ticker: {ticker}, Decision: {decision}, Quantity: {quantity}, Weights: Buy: {buy_weight}, Sell: {sell_weight}, Hold: {hold_weight}")
                    """
                    later we should implement buying_power regulator depending on vix strategy
                    for now in bull: 15000
                    for bear: 5000
                    """
                    
                    if decision == "buy" and float(account.cash) > 15000 and (((quantity + portfolio_qty) * current_price) / portfolio_value) < 0.1:
                        
                        heapq.heappush(buy_heap, (-(buy_weight-(sell_weight + (hold_weight * 0.5))), quantity, ticker))
                    elif (decision == "sell") and portfolio_qty > 0:
                        print(f"Executing SELL order for {ticker}")
                        
                        print(f"Executing quantity of {quantity} for {ticker}")
                        quantity = max(quantity, 1)
                        order = place_order(trading_client, symbol=ticker, side=OrderSide.SELL, quantity=quantity, mongo_client=mongo_client)  # Place order using helper
                        
                        logging.info(f"Executed SELL order for {ticker}: {order}")
                        
                    else:
                        logging.info(f"Holding for {ticker}, no action taken.")
                    

                except Exception as e:
                    logging.error(f"Error processing {ticker}: {e}")

            while (buy_heap or suggestion_heap) and float(account.cash) > 15000:  
                try:
                    if buy_heap:
                        _, quantity, ticker = heapq.heappop(buy_heap)
                        print(f"Executing BUY order for {ticker}")
                        
                        order = place_order(trading_client, symbol=ticker, side=OrderSide.BUY, quantity=quantity, mongo_client=mongo_client)  # Place order using helper
                        
                        logging.info(f"Executed BUY order for {ticker}: {order}")
                        
                        
                    elif suggestion_heap:
                        _, quantity, ticker = heapq.heappop(suggestion_heap)
                        print(f"Executing BUY order for {ticker}")

                        order = place_order(trading_client, symbol=ticker, side=OrderSide.BUY, quantity=quantity, mongo_client=mongo_client)  # Place order using helper

                        logging.info(f"Executed BUY order for {ticker}: {order}")

                    trading_client = TradingClient(API_KEY, API_SECRET)
                    account = trading_client.get_account()
                    
                    
                except:
                    print("Error occurred while executing buy order. Continuing...")
                    break
            
                
            print("Sleeping for 60 seconds...")
            time.sleep(60)

        elif status == "early_hours":
            if early_hour_first_iteration:
                ndaq_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)
                sim_db = mongo_client.trading_simulator
                rank_collection = sim_db.rank
                r_t_c_collection = sim_db.rank_to_coefficient
                for strategy in strategies:
                    rank = rank_collection.find_one({'strategy': strategy.__name__})['rank']
                    coefficient = r_t_c_collection.find_one({'rank': rank})['coefficient']
                    strategy_to_coefficient[strategy.__name__] = coefficient
                    early_hour_first_iteration = False
                    post_hour_first_iteration = True
                logging.info("Market is in early hours. Waiting for 60 seconds.")
            time.sleep(30)

        elif status == "closed":
            
            if post_hour_first_iteration:
                early_hour_first_iteration = True
                post_hour_first_iteration = False
                logging.info("Market is closed. Performing post-market operations.")
            time.sleep(30)
            
        else:
            logging.error("An error occurred while checking market status.")
            time.sleep(60)

if __name__ == "__main__":
    
    main()