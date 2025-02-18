from config import FINANCIAL_PREP_API_KEY, MONGO_DB_USER, MONGO_DB_PASS, API_KEY, API_SECRET, BASE_URL, mongo_url
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
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


from control import mode, train_time_delta_mode, train_time_delta_increment, train_time_delta_multiplicative, train_time_delta_balanced, train_rank_liquidity_limit, train_rank_asset_limit
from control import train_profit_price_change_ratio_d1, train_profit_profit_time_d1, train_profit_price_change_ratio_d2, train_profit_profit_time_d2, train_profit_profit_time_else
from control import train_time_delta, train_loss_price_change_ratio_d1, train_loss_price_change_ratio_d2, train_loss_profit_time_d1, train_loss_profit_time_d2, train_loss_profit_time_else
from control import period_start, period_end, train_tickers, train_stop_loss, train_take_profit, train_start_cash, train_trade_liquidity_limit, train_trade_asset_limit, train_suggestion_heap_limit
import json
from ranking_client import update_ranks
from helper_files.train_client_helper import *
from trading_client import weighted_majority_decision_and_median_quantity

def train():
    """
    initialize
    """
    ticker_price_history = {}
    trading_simulator = {}
    points = {}
    global train_tickers
    """
    need it for time_delta component and we need to adapt time delta for multiple modes - multiplicative, balanced or additive
    """
    for strategy in strategies:
        points[strategy.__name__] = 0
        trading_simulator[strategy.__name__] = {
            "holdings": {},
            "amount_cash": 50000,
            "total_trades": 0,
            "successful_trades": 0,
            "neutral_trades": 0,
            "failed_trades": 0,
            "portfolio_value": 50000
        }
    ideal_period = {}
    time_delta = train_time_delta
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    db = mongo_client.IndicatorsDatabase
    indicator_collection = db.Indicators
    for strategy in strategies:
        period = indicator_collection.find_one({'indicator': strategy.__name__})
        ideal_period[strategy.__name__] = period['ideal_period']
    
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    data_start_date = (start_date - timedelta(days=730)).strftime("%Y-%m-%d")
    print(f"Will be using {train_tickers} for training")
    if len(train_tickers) == 0:
        train_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)

    for ticker in train_tickers:
        try:
            data = yf.Ticker(ticker).history(start=data_start_date, end=period_end, interval="1d")
            logging.info(f'Got data: {ticker}  \t {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}')
            ticker_price_history[ticker] = data
        except:
            data = yf.Ticker(ticker).history(period="max", interval="1d")
            logging.info(f'Got data: {ticker}  \t {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}')
            ticker_price_history[ticker] = data
    
    
    """
    now we have the data loaded, we need to simulate strategy for each day from start day to end day. create a loop that goes from start to end date
    """
    # Now simulate strategy for each day from start date to end date
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    end_date = datetime.strptime(period_end, "%Y-%m-%d")
    current_date = start_date
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)


    print(f"Training on tickers: {train_tickers}")
    
    while current_date <= end_date:
        print(f"Simulating strategies for date: {current_date.strftime('%Y-%m-%d')}")
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        if current_date.strftime('%Y-%m-%d') not in ticker_price_history[train_tickers[0]].index:
            current_date += timedelta(days=1)
            continue
        for ticker in train_tickers:
            """
            what we need to simulate:
            1. strategy - completed
            2. historical data - must give historical data that is ideal period days/months/years before the current date to current date
            3. current_price - get from trading simulator
            4. account_cash - get from trading_simulator
            5. holdings - get from trading_simulator
            6. total_portfolio_value
            """
            if current_date.strftime('%Y-%m-%d') in ticker_price_history[ticker].index:
                daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
                current_price = daily_data['Close']
                for strategy in strategies:
                    historical_data = get_historical_data(ticker, current_date, ideal_period[strategy.__name__], ticker_price_history)
                    account_cash = trading_simulator[strategy.__name__]["amount_cash"]
                    portfolio_qty = trading_simulator[strategy.__name__]["holdings"].get(ticker, {}).get("quantity", 0)
                    total_portfolio_value = trading_simulator[strategy.__name__]["portfolio_value"]
                    decision, qty = simulate_strategy(
                        strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value
                    )
                    print(f"{strategy.__name__} - {decision} - {qty} - {ticker}")
                    """
                    now simulate the trade
                    """
                    if decision == "buy" and trading_simulator[strategy.__name__]["amount_cash"] > train_rank_liquidity_limit and qty > 0 and ((portfolio_qty + qty) * current_price) / total_portfolio_value < train_rank_asset_limit:
                        trading_simulator[strategy.__name__]["amount_cash"] -= qty * current_price
                    
                        if ticker in trading_simulator[strategy.__name__]["holdings"]:
                            trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] += qty
                        else:
                            trading_simulator[strategy.__name__]["holdings"][ticker] = {"quantity": qty}
                    
                        trading_simulator[strategy.__name__]["holdings"][ticker]["price"] = current_price
                        trading_simulator[strategy.__name__]["total_trades"] += 1
                        
                    elif decision == "sell" and trading_simulator[strategy.__name__]["holdings"].get(ticker, {}).get("quantity", 0) >= qty:
                        trading_simulator[strategy.__name__]["amount_cash"] += qty * current_price
                        ratio = current_price / trading_simulator[strategy.__name__]["holdings"][ticker]["price"]
                        if current_price > trading_simulator[strategy.__name__]["holdings"][ticker]["price"]:
                            trading_simulator[strategy.__name__]["successful_trades"] += 1
                            if ratio < train_profit_price_change_ratio_d1:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d1
                            elif ratio < train_profit_price_change_ratio_d2:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d2
                            else:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_else
                            """
                            points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d1
                            """
                        elif current_price == trading_simulator[strategy.__name__]["holdings"][ticker]["price"]:
                            trading_simulator[strategy.__name__]["neutral_trades"] += 1
                        else:
                            trading_simulator[strategy.__name__]["failed_trades"] += 1
                            if ratio > train_loss_price_change_ratio_d1:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_d1
                            elif ratio > train_loss_price_change_ratio_d2:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_d2
                            else:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_else

                        trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] -= qty
                        if trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] == 0:
                            del trading_simulator[strategy.__name__]["holdings"][ticker]
                        elif trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] < 0:
                            Exception("Quantity cannot be negative")
                        trading_simulator[strategy.__name__]["total_trades"] += 1
        active_count, trading_simulator = local_update_portfolio_values(current_date, strategies, trading_simulator, ticker_price_history) 
        """
        log history of trading_simulator and points
        """
        logging.info(f"Trading simulator: {trading_simulator}")
        logging.info(f"Points: {points}")
        logging.info(f"Date: {current_date.strftime('%Y-%m-%d')}")
        logging.info(f"time_delta: {time_delta}")
        logging.info(f"Active count: {active_count}")
        logging.info("-------------------------------------------------")
        
        """
        Update time_delta based on the mode
        """
        if train_time_delta_mode == 'additive':
            time_delta += train_time_delta_increment
        elif train_time_delta_mode == 'multiplicative':
            time_delta *= train_time_delta_multiplicative
        elif train_time_delta_mode == 'balanced':
            time_delta += train_time_delta_balanced * time_delta


        current_date += timedelta(days=1)
        time.sleep(5)
        
    results = {
        "trading_simulator": trading_simulator,
        "points": points,
        "date": current_date.strftime('%Y-%m-%d'),
        "time_delta": time_delta
        }
        
    with open('training_results.json', 'w') as json_file:
        json.dump(results, json_file, indent=4)

    """
    output onto console top 10 strategies with highest portfolio values and top 10 strategies with highest points
    """
    top_portfolio_values = heapq.nlargest(10, trading_simulator.items(), key=lambda x: x[1]["portfolio_value"])
    top_points = heapq.nlargest(10, points.items(), key=lambda x: x[1])
    print("Top 10 strategies with highest portfolio values")
    for strategy, value in top_portfolio_values:
        print(f"{strategy} - {value['portfolio_value']}")
    print("Top 10 strategies with highest points")
    for strategy, value in top_points:
        print(f"{strategy} - {value}")
    print("Training completed.")
   
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
def test():
    with open('training_results.json', 'r') as json_file:
         results = json.load(json_file)
         trading_simulator = results['trading_simulator']
         points = results['points']
         date = results['date']
         time_delta = results['time_delta']
    print(trading_simulator)
    print(points)
    print(date)
    print(time_delta)
    ticker_price_history = {}
    ideal_period = {}
    
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    db = mongo_client.trading_simulator
    r_t_c = db.rank_to_coefficient

    rank_to_coefficient = {}
    for doc in r_t_c.find({}):
        rank_to_coefficient[doc['rank']] = doc['coefficient']
    print("Rank Coefficient Retrieved")

    db = mongo_client.IndicatorsDatabase
    indicator_collection = db.Indicators
    for strategy in strategies:
        period = indicator_collection.find_one({'indicator': strategy.__name__})
        ideal_period[strategy.__name__] = period['ideal_period']
    global train_tickers
    if not train_tickers:
        train_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    data_start_date = (start_date - timedelta(days=730)).strftime("%Y-%m-%d")
    print(f"Will be using {train_tickers} for training")
    
    for ticker in train_tickers:
        try:
            data = yf.Ticker(ticker).history(start=data_start_date, end=period_end, interval="1d")
            logging.info(f'Got data: {ticker}  \t {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}')
            ticker_price_history[ticker] = data
        except:
            data = yf.Ticker(ticker).history(period="max", interval="1d")
            logging.info(f'Got data: {ticker}  \t {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}')
            ticker_price_history[ticker] = data
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    end_date = datetime.strptime(period_end, "%Y-%m-%d")
    current_date = start_date
    rank = {}
    """
    we assign strategy a rank
    """
    def update_ranks():
        q = []
        for strategy in strategies:
            if points[strategy.__name__] > 0:
                heapq.heappush(q, (points[strategy.__name__] * 2 + trading_simulator[strategy.__name__]["portfolio_value"], trading_simulator[strategy.__name__]["successful_trades"] - trading_simulator[strategy.__name__]["failed_trades"], trading_simulator[strategy.__name__]["amount_cash"], strategy.__name__))
            else:
                heapq.heappush(q, (trading_simulator[strategy.__name__]["portfolio_value"], trading_simulator[strategy.__name__]["successful_trades"] - trading_simulator[strategy.__name__]["failed_trades"], trading_simulator[strategy.__name__]["amount_cash"], strategy.__name__))
        coeff_rank = 1
        while q:
            _, _, _, strategy_name = heapq.heappop(q)
            rank[strategy_name] = coeff_rank
            coeff_rank+=1
            
        return rank

    strategy_to_coefficient = {}
    account = {
        "holdings": {},
        "cash": train_start_cash,
        "trades" : [],
        "total_portfolio_value": train_start_cash

    }
    rank = update_ranks()
    print(rank)
    account_values = pd.Series(index=pd.date_range(start=start_date, end=end_date))
    
    while current_date <= end_date:
        print(f"Simulating strategies for date: {current_date.strftime('%Y-%m-%d')}")
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        if current_date.strftime('%Y-%m-%d') not in ticker_price_history[train_tickers[0]].index:
            current_date += timedelta(days=1)
            continue
        
        """
        simulate early hour operations
        - retrieve rank and assign to new coefficient
        """
        print(f"rank_to_coefficient: {rank_to_coefficient}")
        
        for strategy in strategies:
            strategy_to_coefficient[strategy.__name__] = rank_to_coefficient[rank[strategy.__name__]]
        print(f"strategy_to_coefficient: {strategy_to_coefficient}")
        """
        simulate trading
        check stop loss + take profit and buy & sell in accordance to ranking
        """
        buy_heap, suggestion_heap = [], []
        for ticker in train_tickers:
            if current_date.strftime('%Y-%m-%d') in ticker_price_history[ticker].index:
                daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
                current_price = daily_data['Close']
                """
                check stop loss + take profit
                """
                if ticker in account["holdings"]:
                    if account["holdings"][ticker]["quantity"] > 0:
                        if current_price < account["holdings"][ticker]["stop_loss"] or current_price > account["holdings"][ticker]["take_profit"]:
                            account["trades"].append({"symbol": ticker, "quantity": account["holdings"][ticker]["quantity"], "price": current_price, "action": "sell"})
                            account["cash"] += account["holdings"][ticker]["quantity"] * current_price
                            del account["holdings"][ticker]
                """
                now simulate strategies and store 
                """
                
                decisions_and_quantities = []
                portfolio_qty = 0.0
                for strategy in strategies:
                    historical_data = get_historical_data(ticker, current_date, ideal_period[strategy.__name__], ticker_price_history)
                    account_cash = account["cash"]
                    portfolio_qty = account["holdings"][ticker]["quantity"] if ticker in account["holdings"] else 0
                    total_portfolio_value = account["total_portfolio_value"]
                    decision, qty = simulate_strategy(strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value)
                    weight = strategy_to_coefficient[strategy.__name__]
                    decisions_and_quantities.append((decision, qty, weight))
                decision, quantity, buy_weight, sell_weight, hold_weight = weighted_majority_decision_and_median_quantity(decisions_and_quantities)
                print(f"Ticker: {ticker}, Decision: {decision}, Quantity: {quantity}, Buy Weight: {buy_weight}, Sell Weight: {sell_weight}, Hold Weight: {hold_weight}")
                if decision == 'buy' and  ((portfolio_qty + quantity) * current_price) / account["total_portfolio_value"] <= train_trade_asset_limit:
                    heapq.heappush(buy_heap, (-(buy_weight-(sell_weight + (hold_weight * 0.5))), quantity, ticker))
                elif decision == 'sell' and ticker in account["holdings"]:
                    quantity = max(quantity, 1)
                    """
                    execute sell on spot
                    """
                    account["trades"].append({"symbol": ticker, "quantity": quantity, "price": current_price, "action": "sell", "date": current_date.strftime('%Y-%m-%d')})
                    quantity = account["holdings"][ticker]["quantity"]
                    account["cash"] += quantity * current_price
                    del account["holdings"][ticker]
                    
                elif portfolio_qty == 0.0 and buy_weight > sell_weight and (((quantity + portfolio_qty) * current_price) / account["total_portfolio_value"]) < trade_asset_limit and float(account["cash"]) >= train_trade_liquidity_limit:
                    max_investment = account["total_portfolio_value"] * train_trade_asset_limit
                    buy_quantity = min(int(max_investment // current_price), int(account["cash"] // current_price))
                    if buy_weight > train_suggestion_heap_limit:
                        buy_quantity = max(2, buy_quantity)
                        buy_quantity = buy_quantity // 2
                        heapq.heappush(suggestion_heap, (-(buy_weight - sell_weight), buy_quantity, ticker))

        while (buy_heap or suggestion_heap) and float(account["cash"]) > train_trade_liquidity_limit:
            if buy_heap and float(account["cash"]) > train_trade_liquidity_limit:
                _, quantity, ticker = heapq.heappop(buy_heap)
                print(f"Executing BUY order for {ticker} of quantity {quantity}")
                current_price = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]['Close']
                account["trades"].append({"symbol": ticker, "quantity": quantity, "price": current_price, "action": "buy", "date": current_date.strftime('%Y-%m-%d')})
                account["cash"] -= quantity * current_price
                account["holdings"][ticker] = {"quantity": quantity, "price": current_price, "stop_loss": current_price * (1 - train_stop_loss), "take_profit": current_price * (1 + train_take_profit)}
            elif suggestion_heap and float(account["cash"]) > train_trade_liquidity_limit:
                _, quantity, ticker = heapq.heappop(suggestion_heap)
                print(f"Executing BUY order for {ticker} of quantity {quantity}")
                current_price = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]['Close']
                account["trades"].append({"symbol": ticker, "quantity": quantity, "price": current_price, "action": "buy", "date": current_date.strftime('%Y-%m-%d')})
                account["cash"] -= quantity * current_price
                account["holdings"][ticker] = {"quantity": quantity, "price": current_price, "stop_loss": current_price * (1 - train_stop_loss), "take_profit": current_price * (1 + train_take_profit)}
                
        buy_heap = []
        suggestion_heap = []
        logging.info("-------------------------------------------------")
        logging.info(f"Account Cash: ${account['cash']:,.2f}")
        logging.info(f"Trades: {account['trades']}")
        logging.info(f"Holdings: {account['holdings']}")
        logging.info(f"Total Portfolio Value: ${account['total_portfolio_value']:,.2f}")
        
        logging.info("-------------------------------------------------")
        time.sleep(5)
        """
        simulate ranking
        """
        for ticker in train_tickers:
            """
            what we need to simulate:
            1. strategy - completed
            2. historical data - must give historical data that is ideal period days/months/years before the current date to current date
            3. current_price - get from trading simulator
            4. account_cash - get from trading_simulator
            5. holdings - get from trading_simulator
            6. total_portfolio_value
            """
            if current_date.strftime('%Y-%m-%d') in ticker_price_history[ticker].index:
                daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
                current_price = daily_data['Close']
                for strategy in strategies:
                    historical_data = get_historical_data(ticker, current_date, ideal_period[strategy.__name__], ticker_price_history)
                    account_cash = trading_simulator[strategy.__name__]["amount_cash"]
                    portfolio_qty = trading_simulator[strategy.__name__]["holdings"].get(ticker, {}).get("quantity", 0)
                    total_portfolio_value = trading_simulator[strategy.__name__]["portfolio_value"]
                    decision, qty = simulate_strategy(
                        strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value
                    )
                    print(f"{strategy.__name__} - {decision} - {qty} - {ticker}")
                    """
                    now simulate the trade
                    """
                    if decision == "buy" and trading_simulator[strategy.__name__]["amount_cash"] > train_rank_liquidity_limit and qty > 0 and ((portfolio_qty + qty) * current_price) / total_portfolio_value < train_rank_asset_limit:
                        trading_simulator[strategy.__name__]["amount_cash"] -= qty * current_price
                    
                        if ticker in trading_simulator[strategy.__name__]["holdings"]:
                            trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] += qty
                        else:
                            trading_simulator[strategy.__name__]["holdings"][ticker] = {"quantity": qty}
                    
                        trading_simulator[strategy.__name__]["holdings"][ticker]["price"] = current_price
                        trading_simulator[strategy.__name__]["total_trades"] += 1
                        
                    elif decision == "sell" and trading_simulator[strategy.__name__]["holdings"].get(ticker, {}).get("quantity", 0) >= qty:
                        trading_simulator[strategy.__name__]["amount_cash"] += qty * current_price
                        ratio = current_price / trading_simulator[strategy.__name__]["holdings"][ticker]["price"]
                        if current_price > trading_simulator[strategy.__name__]["holdings"][ticker]["price"]:
                            trading_simulator[strategy.__name__]["successful_trades"] += 1
                            if ratio < train_profit_price_change_ratio_d1:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d1
                            elif ratio < train_profit_price_change_ratio_d2:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d2
                            else:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_else
                            """
                            points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d1
                            """
                        elif current_price == trading_simulator[strategy.__name__]["holdings"][ticker]["price"]:
                            trading_simulator[strategy.__name__]["neutral_trades"] += 1
                        else:
                            trading_simulator[strategy.__name__]["failed_trades"] += 1
                            if ratio > train_loss_price_change_ratio_d1:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_d1
                            elif ratio > train_loss_price_change_ratio_d2:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_d2
                            else:
                                points[strategy.__name__] = points.get(strategy.__name__, 0) + -time_delta * train_loss_profit_time_else

                        trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] -= qty
                        if trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] == 0:
                            del trading_simulator[strategy.__name__]["holdings"][ticker]
                        elif trading_simulator[strategy.__name__]["holdings"][ticker]["quantity"] < 0:
                            Exception("Quantity cannot be negative")
                        trading_simulator[strategy.__name__]["total_trades"] += 1
        active_count = local_update_portfolio_values(current_date, strategies, trading_simulator, ticker_price_history) 
        """
        log history of trading_simulator and points
        """
        logging.info(f"Trading simulator: {trading_simulator}")
        logging.info(f"Points: {points}")
        logging.info(f"Date: {current_date.strftime('%Y-%m-%d')}")
        logging.info(f"time_delta: {time_delta}")
        logging.info(f"Active count: {active_count}")
        logging.info("-------------------------------------------------")
        
        """
        Update time_delta based on the mode
        """
        if train_time_delta_mode == 'additive':
            time_delta += train_time_delta_increment
        elif train_time_delta_mode == 'multiplicative':
            time_delta *= train_time_delta_multiplicative
        elif train_time_delta_mode == 'balanced':
            time_delta += train_time_delta_balanced * time_delta
        """
        simulate closing actions for both 
        - updating total portfolio value of account
        - update portfolio_value of each strategies
        - update ranks
        """
        total_value = account["cash"]
        for ticker in account["holdings"]:
            daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
            current_price = daily_data['Close']
            total_value = total_value + account["holdings"][ticker]["quantity"] * current_price
        account["total_portfolio_value"] = total_value

        active_count, trading_simulator = local_update_portfolio_values(current_date, strategies, trading_simulator, ticker_price_history)
        
        rank = update_ranks()

        total_value = sum([holding["quantity"] * ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]['Close'] for ticker, holding in trading_simulator[strategy.__name__]["holdings"].items()]) + trading_simulator[strategy.__name__]["amount_cash"]
        account_values[current_date] = total_value

        current_date += timedelta(days=1)
        time.sleep(5)
    
    """
    Generate tear sheet
    """
    # metrics = calculate_metrics(account_values)
    # print(metrics)
    generate_tear_sheet(account_values, 'Strategy_vs_SPY')

    """
    print some stats
    """
    print("Testing Completed")
    print("-------------------------------------------------")
    print(f"Account Cash: ${account['cash']:,.2f}")
    print(f"Trades: {account['trades']}")
    print(f"Holdings: {account['holdings']}")
    print(f"Total Portfolio Value: ${account['total_portfolio_value']:,.2f}")
    
    print("-------------------------------------------------")


if __name__ == "__main__":
    if mode == 'train':
        train()
    elif mode == 'push':
        push()
    elif mode == 'test':
        test()

