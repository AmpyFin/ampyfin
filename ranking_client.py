from polygon import RESTClient
from config import POLYGON_API_KEY, FINANCIAL_PREP_API_KEY, MONGO_DB_USER, MONGO_DB_PASS, API_KEY, API_SECRET, BASE_URL, mongo_url
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import alpaca
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.stream import TradingStream
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import (
    StockBarsRequest,
    StockTradesRequest,
    StockQuotesRequest
)
from alpaca.trading.requests import (
    GetAssetsRequest, 
    MarketOrderRequest, 
    LimitOrderRequest, 
    StopOrderRequest, 
    StopLimitOrderRequest, 
    TakeProfitRequest, 
    StopLossRequest, 
    TrailingStopOrderRequest, 
    GetOrdersRequest, 
    ClosePositionRequest
)
from alpaca.trading.enums import ( 
    AssetStatus, 
    AssetExchange, 
    OrderSide, 
    OrderType, 
    TimeInForce, 
    OrderClass, 
    QueryOrderStatus
)
from alpaca.common.exceptions import APIError
from strategies.talib_indicators import *
import math
import yfinance as yf
import logging
from collections import Counter
from trading_client import market_status
from helper_files.client_helper import strategies, get_latest_price, get_ndaq_tickers, dynamic_period_selector
import time
from datetime import datetime 
import heapq 
import certifi
ca = certifi.where()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('rank_system.log'),  # Log messages to a file
        logging.StreamHandler()             # Log messages to the console
    ]
)

def process_ticker(ticker, mongo_client):
   try:
      
      current_price = None
      
      while current_price is None:
         try:
            current_price = get_latest_price(ticker)
         except Exception as fetch_error:
            logging.warning(f"Error fetching price for {ticker}. Retrying... {fetch_error}")
            time.sleep(10)
      
      indicator_tb = mongo_client.IndicatorsDatabase
      indicator_collection = indicator_tb.Indicators
      for strategy in strategies:
         historical_data = None
         while historical_data is None:
            try:
               period = indicator_collection.find_one({'indicator': strategy.__name__})
               historical_data = get_data(ticker, mongo_client, period['ideal_period'])
            except Exception as fetch_error:
               logging.warning(f"Error fetching historical data for {ticker}. Retrying... {fetch_error}")
               time.sleep(60)
         db = mongo_client.trading_simulator  
         holdings_collection = db.algorithm_holdings
         print(f"Processing {strategy.__name__} for {ticker}")
         strategy_doc = holdings_collection.find_one({"strategy": strategy.__name__})
         if not strategy_doc:
            logging.warning(f"Strategy {strategy.__name__} not found in database. Skipping.")
            continue

         account_cash = strategy_doc["amount_cash"]
         total_portfolio_value = strategy_doc["portfolio_value"]

         
         portfolio_qty = strategy_doc["holdings"].get(ticker, {}).get("quantity", 0)

         simulate_trade(ticker, strategy, historical_data, current_price,
                        account_cash, portfolio_qty, total_portfolio_value, mongo_client)
         
      print(f"{ticker} processing completed.")
   except Exception as e:
      logging.error(f"Error in thread for {ticker}: {e}")

def simulate_trade(ticker, strategy, historical_data, current_price, account_cash, portfolio_qty, total_portfolio_value, mongo_client):
   """
   Simulates a trade based on the given strategy and updates MongoDB.
   """
    
   # Simulate trading action from strategy
   print(f"Simulating trade for {ticker} with strategy {strategy.__name__} and quantity of {portfolio_qty}")
   action, quantity = simulate_strategy(strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value)
   
   # MongoDB setup
   
   db = mongo_client.trading_simulator
   holdings_collection = db.algorithm_holdings
   points_collection = db.points_tally
   
   # Find the strategy document in MongoDB
   strategy_doc = holdings_collection.find_one({"strategy": strategy.__name__})
   holdings_doc = strategy_doc.get("holdings", {})
   time_delta = db.time_delta.find_one({})['time_delta']
   
   
   # Update holdings and cash based on trade action
   if action in ["buy"] and strategy_doc["amount_cash"] - quantity * current_price > 15000 and quantity > 0 and ((portfolio_qty + quantity) * current_price) / total_portfolio_value < 0.10:
      logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
      # Calculate average price if already holding some shares of the ticker
      if ticker in holdings_doc:
         current_qty = holdings_doc[ticker]["quantity"]
         new_qty = current_qty + quantity
         average_price = (holdings_doc[ticker]["price"] * current_qty + current_price * quantity) / new_qty
      else:
         new_qty = quantity
         average_price = current_price

      # Update the holdings document for the ticker. 
      holdings_doc[ticker] = {
            "quantity": new_qty,
            "price": average_price
      }

      # Deduct the cash used for buying and increment total trades
      holdings_collection.update_one(
         {"strategy": strategy.__name__},
         {
            "$set": {
                  "holdings": holdings_doc,
                  "amount_cash": strategy_doc["amount_cash"] - quantity * current_price,
                  "last_updated": datetime.now()
            },
            "$inc": {"total_trades": 1}
         },
         upsert=True
      )
      

   elif action in ["sell"] and str(ticker) in holdings_doc and holdings_doc[str(ticker)]["quantity"] > 0:
      
      logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
      current_qty = holdings_doc[ticker]["quantity"]
        
      # Ensure we do not sell more than we have
      sell_qty = min(quantity, current_qty)
      holdings_doc[ticker]["quantity"] = current_qty - sell_qty
      
      price_change_ratio = current_price / holdings_doc[ticker]["price"] if ticker in holdings_doc else 1
      
      

      if current_price > holdings_doc[ticker]["price"]:
         #increment successful trades
         holdings_collection.update_one(
            {"strategy": strategy.__name__},
            {"$inc": {"successful_trades": 1}},
            upsert=True
         )
         
         # Calculate points to add if the current price is higher than the purchase price
         if price_change_ratio < 1.05:
            points = time_delta * 1
         elif price_change_ratio < 1.1:
            points = time_delta * 1.5
         else:
            points = time_delta * 2
         
      else:
         # Calculate points to deduct if the current price is lower than the purchase price
         if holdings_doc[ticker]["price"] == current_price:
            holdings_collection.update_one(
               {"strategy": strategy.__name__},
               {"$inc": {"neutral_trades": 1}}
            )
            
         else:   
            
            holdings_collection.update_one(
               {"strategy": strategy.__name__},
               {"$inc": {"failed_trades": 1}},
               upsert=True
            )
         
         if price_change_ratio > 0.975:
            points = -time_delta * 1
         elif price_change_ratio > 0.95:
            points = -time_delta * 1.5
         else:
            points = -time_delta * 2
         
      # Update the points tally
      points_collection.update_one(
         {"strategy": strategy.__name__},
         {
            "$set" : {
               "last_updated": datetime.now()
            },
            "$inc": {"total_points": points}
         },
         upsert=True
      )
      if holdings_doc[ticker]["quantity"] == 0:      
         del holdings_doc[ticker]
      # Update cash after selling
      holdings_collection.update_one(
         {"strategy": strategy.__name__},
         {
            "$set": {
               "holdings": holdings_doc,
               "amount_cash": strategy_doc["amount_cash"] + sell_qty * current_price,
               "last_updated": datetime.now()
            },
            "$inc": {"total_trades": 1}
         },
         upsert=True
      )

        
      # Remove the ticker if quantity reaches zero
      if holdings_doc[ticker]["quantity"] == 0:      
         del holdings_doc[ticker]
        
   else:
      logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
   print(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
   # Close the MongoDB connection

def update_portfolio_values(client):
   """
   still need to implement.
   we go through each strategy and update portfolio value buy cash + summation(holding * current price)
   """
   
   db = client.trading_simulator  
   holdings_collection = db.algorithm_holdings
   # Update portfolio values
   for strategy_doc in holdings_collection.find({}):
      # Calculate the portfolio value for the strategy
      portfolio_value = strategy_doc["amount_cash"]
      
      for ticker, holding in strategy_doc["holdings"].items():
          
          # Get the current price of the ticker from the Polygon API
          current_price = None
          while current_price is None:
            try:
               current_price = get_latest_price(ticker)
            except:
               print(f"Error fetching price for {ticker}. Retrying...")
          print(f"Current price of {ticker}: {current_price}")
          # Calculate the value of the holding
          holding_value = holding["quantity"] * current_price
          # Add the holding value to the portfolio value
          portfolio_value += holding_value
          
      # Update the portfolio value in the strategy document
      holdings_collection.update_one({"strategy": strategy_doc["strategy"]}, {"$set": {"portfolio_value": portfolio_value}}, upsert=True)

   # Update MongoDB with the modified strategy documents
   

def update_ranks(client):
   """"
   based on portfolio values, rank the strategies to use for actual trading_simulator
   """
   
   db = client.trading_simulator
   points_collection = db.points_tally
   rank_collection = db.rank
   algo_holdings = db.algorithm_holdings
   """
   delete all documents in rank collection first
   """
   rank_collection.delete_many({})
   """
   now update rank based on successful_trades - failed
   """
   q = []
   for strategy_doc in algo_holdings.find({}):
      """
      based on (points_tally (less points pops first), failed-successful(more negtive pops first), portfolio value (less value pops first), and then strategy_name), we add to heapq.
      """
      strategy_name = strategy_doc["strategy"]
      if strategy_name == "test" or strategy_name == "test_strategy":
         continue
      if points_collection.find_one({"strategy": strategy_name})["total_points"] > 0:
         
         heapq.heappush(q, (points_collection.find_one({"strategy": strategy_name})["total_points"] * 2 + (strategy_doc["portfolio_value"]), strategy_doc["successful_trades"] - strategy_doc["failed_trades"], strategy_doc["amount_cash"], strategy_doc["strategy"]))
      else:
         heapq.heappush(q, (strategy_doc["portfolio_value"], strategy_doc["successful_trades"] - strategy_doc["failed_trades"], strategy_doc["amount_cash"], strategy_doc["strategy"]))
   rank = 1
   while q:
      
      _, _, _, strategy_name = heapq.heappop(q)
      rank_collection.insert_one({"strategy": strategy_name, "rank": rank})
      rank+=1
   
   """
   Delete historical database so new one can be used tomorrow
   """
   db = client.HistoricalDatabase
   collection = db.HistoricalDatabase
   collection.delete_many({})
   print("Successfully updated ranks")
   print("Successfully deleted historical database")
   
def main():  
   """  
   Main function to control the workflow based on the market's status.  
   """  
   ndaq_tickers = []  
   early_hour_first_iteration = True
   post_market_hour_first_iteration = True
   
   
   while True: 
      mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
      status = mongo_client.market_data.market_status.find_one({})["market_status"]
      
      if status == "open":  
         
         if not ndaq_tickers:
            logging.info("Market is open. Processing strategies.")  
            ndaq_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)

         threads = []

         for ticker in ndaq_tickers:
            thread = threading.Thread(target=process_ticker, args=(ticker, mongo_client))
            threads.append(thread)
            thread.start()

         # Wait for all threads to complete
         for thread in threads:
            thread.join()

         
         

         logging.info("Finished processing all strategies. Waiting for 120 seconds.")
         time.sleep(120)  
      
      elif status == "early_hours":  
            if early_hour_first_iteration is True:  
               
               ndaq_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)  
               early_hour_first_iteration = False  
               post_market_hour_first_iteration = True
               logging.info("Market is in early hours. Waiting for 60 seconds.")  
            time.sleep(60)  
  
      elif status == "closed":  
         
        
        if post_market_hour_first_iteration is True:
            early_hour_first_iteration = True
            logging.info("Market is closed. Performing post-market analysis.") 
            post_market_hour_first_iteration = False
            #increment time_delta in database by 0.01
            
            mongo_client.trading_simulator.time_delta.update_one({}, {"$inc": {"time_delta": 0.01}})
            
            
            #Update ranks
            update_portfolio_values(mongo_client)
            update_ranks(mongo_client)
        time.sleep(60)  
      else:  
        logging.error("An error occurred while checking market status.")  
        time.sleep(60)
      mongo_client.close()
   
  
if __name__ == "__main__":  
   main()