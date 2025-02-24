from config import *
from utils import * 
from trading_client import weighted_majority_decision_and_median_quantity
import certifi
from pymongo import MongoClient

import os
import heapq
import logging

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

file_handler = logging.FileHandler(os.path.join(logs_dir, 'testing.log'))
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

ca = certifi.where()
mongo_client = MongoClient(mongo_url, tlsCAFile=ca)

def initialize_test_account():
    """
    Initializes the test trading account with starting parameters
    """
    return {
        "holdings": {},
        "cash": train_start_cash,
        "trades": [],
        "total_portfolio_value": train_start_cash
    }

def check_stop_loss_take_profit(account, ticker, current_price):
    """
    Checks and executes stop loss and take profit orders
    """
    if ticker in account["holdings"]:
        if account["holdings"][ticker]["quantity"] > 0:
            if (current_price < account["holdings"][ticker]["stop_loss"] or 
                current_price > account["holdings"][ticker]["take_profit"]):
                account["trades"].append({
                    "symbol": ticker,
                    "quantity": account["holdings"][ticker]["quantity"],
                    "price": current_price,
                    "action": "sell"
                })
                account["cash"] += account["holdings"][ticker]["quantity"] * current_price
                del account["holdings"][ticker]
    return account

def execute_buy_orders(buy_heap, suggestion_heap, account, ticker_price_history, current_date):
    """
    Executes buy orders from the buy and suggestion heaps
    """
    while (buy_heap or suggestion_heap) and float(account["cash"]) > train_trade_liquidity_limit:
        if buy_heap and float(account["cash"]) > train_trade_liquidity_limit:
            heap = buy_heap
        elif suggestion_heap and float(account["cash"]) > train_trade_liquidity_limit:
            heap = suggestion_heap
        else:
            break
            
        _, quantity, ticker = heapq.heappop(heap)
        # print(f"Executing BUY order for {ticker} of quantity {quantity}")
        current_price = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]['Close']
        
        account["trades"].append({
            "symbol": ticker,
            "quantity": quantity,
            "price": current_price,
            "action": "buy",
            "date": current_date.strftime('%Y-%m-%d')
        })
        
        account["cash"] -= quantity * current_price
        account["holdings"][ticker] = {
            "quantity": quantity,
            "price": current_price,
            "stop_loss": current_price * (1 - train_stop_loss),
            "take_profit": current_price * (1 + train_take_profit)
        }
    
    return account

def update_strategy_ranks(strategies, points, trading_simulator):
    """
    Updates strategy rankings based on performance
    """
    rank = {}
    q = []
    for strategy in strategies:
        if points[strategy.__name__] > 0:
            score = (points[strategy.__name__] * 2 + 
                    trading_simulator[strategy.__name__]["portfolio_value"])
        else:
            score = trading_simulator[strategy.__name__]["portfolio_value"]
            
        heapq.heappush(q, (
            score,
            trading_simulator[strategy.__name__]["successful_trades"] - 
            trading_simulator[strategy.__name__]["failed_trades"],
            trading_simulator[strategy.__name__]["amount_cash"],
            strategy.__name__
        ))
    
    coeff_rank = 1
    while q:
        _, _, _, strategy_name = heapq.heappop(q)
        rank[strategy_name] = coeff_rank
        coeff_rank += 1
        
    return rank

def test():
    """
    Runs the testing phase of the trading simulator.
    """
    logger.info("Starting testing phase...")

    # Get rank coefficients from database
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    db = mongo_client.trading_simulator
    r_t_c = db.rank_to_coefficient
    rank_to_coefficient = {doc['rank']: doc['coefficient'] for doc in r_t_c.find({})}
    logger.info("Rank coefficients retrieved from database.")

    # Load saved results
    logger.info("Loading saved training results...")
    with open(os.path.join(results_dir, 'training_results.json'), 'r') as json_file:
        results = json.load(json_file)
        trading_simulator = results['trading_simulator']
        points = results['points']
        time_delta = results['time_delta']
    logger.info("Training results loaded successfully.")

    # Initialize simulation data
    ticker_price_history, ideal_period = initialize_simulation(
        period_start, period_end, train_tickers, mongo_client, FINANCIAL_PREP_API_KEY, logger
    )

    # Initialize testing variables
    strategy_to_coefficient = {}
    account = initialize_test_account()
    rank = update_strategy_ranks(strategies, points, trading_simulator)
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    end_date = datetime.strptime(period_end, "%Y-%m-%d")
    current_date = start_date
    account_values = pd.Series(index=pd.date_range(start=start_date, end=end_date))
    logger.info(f"Testing period: {start_date} to {end_date}")

    while current_date <= end_date:
        logger.info(f"Processing date: {current_date.strftime('%Y-%m-%d')}")

        # Skip non-trading days
        if current_date.weekday() >= 5 or current_date.strftime('%Y-%m-%d') not in ticker_price_history[train_tickers[0]].index:
            logger.info(f"Skipping {current_date.strftime('%Y-%m-%d')} (weekend or missing data).")
            current_date += timedelta(days=1)
            continue

        # Update strategy coefficients
        for strategy in strategies:
            strategy_to_coefficient[strategy.__name__] = rank_to_coefficient[rank[strategy.__name__]]
        # logger.info(f"Strategy coefficients updated: {strategy_to_coefficient}")

        # Process trading day
        buy_heap, suggestion_heap = [], []
        for ticker in train_tickers:
            if current_date.strftime('%Y-%m-%d') in ticker_price_history[ticker].index:
                daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
                current_price = daily_data['Close']
                # logger.info(f"{ticker} - Current price: {current_price}")

                # Check stop loss and take profit
                account = check_stop_loss_take_profit(account, ticker, current_price)

                # Get strategy decisions
                decisions_and_quantities = []
                portfolio_qty = account["holdings"].get(ticker, {}).get("quantity", 0)

                for strategy in strategies:
                    historical_data = get_historical_data(ticker, current_date, ideal_period[strategy.__name__], ticker_price_history)
                    decision, qty = simulate_strategy(
                        strategy, ticker, current_price, historical_data,
                        account["cash"], portfolio_qty, account["total_portfolio_value"]
                    )
                    weight = strategy_to_coefficient[strategy.__name__]
                    decisions_and_quantities.append((decision, qty, weight))

                # Process weighted decisions
                decision, quantity, buy_weight, sell_weight, hold_weight = weighted_majority_decision_and_median_quantity(decisions_and_quantities)
                logger.info(f"{ticker} - Decision: {decision}, Quantity: {quantity}, Buy Weight: {buy_weight}, Sell Weight: {sell_weight}, Hold Weight: {hold_weight}")

                # Execute trading decisions
                if decision == 'buy' and ((portfolio_qty + quantity) * current_price) / account["total_portfolio_value"] <= train_trade_asset_limit:
                    heapq.heappush(buy_heap, (-(buy_weight - (sell_weight + (hold_weight * 0.5))), quantity, ticker))
                
                elif decision == 'sell' and ticker in account["holdings"]:
                    quantity = max(quantity, 1)
                    quantity = account["holdings"][ticker]["quantity"]
                    account["trades"].append({
                        "symbol": ticker,
                        "quantity": quantity,
                        "price": current_price,
                        "action": "sell",
                        "date": current_date.strftime('%Y-%m-%d')
                    })
                    account["cash"] += quantity * current_price
                    del account["holdings"][ticker]
                    logger.info(f"{ticker} - Sold {quantity} shares at ${current_price}")

                elif (portfolio_qty == 0.0 and buy_weight > sell_weight and 
                      ((quantity * current_price) / account["total_portfolio_value"]) < trade_asset_limit and 
                      float(account["cash"]) >= train_trade_liquidity_limit):
                    max_investment = account["total_portfolio_value"] * train_trade_asset_limit
                    buy_quantity = min(int(max_investment // current_price), int(account["cash"] // current_price))
                    if buy_weight > train_suggestion_heap_limit:
                        buy_quantity = max(2, buy_quantity)
                        buy_quantity = buy_quantity // 2
                        heapq.heappush(suggestion_heap, (-(buy_weight - sell_weight), buy_quantity, ticker))
        
        # Execute buy orders
        account = execute_buy_orders(buy_heap, suggestion_heap, account, ticker_price_history, current_date)

        # Simulate ranking updates
        trading_simulator, points = simulate_trading_day(
            current_date, strategies, trading_simulator, points,
            time_delta, ticker_price_history, train_tickers, ideal_period, logger
        )

        # Update portfolio values
        active_count, trading_simulator = local_update_portfolio_values(
            current_date, strategies, trading_simulator, ticker_price_history, logger
        )

        # Update time delta
        time_delta = update_time_delta(time_delta, train_time_delta_mode)

        # Calculate and update total portfolio value
        total_value = account["cash"]
        for ticker in account["holdings"]:
            current_price = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]['Close']
            total_value += account["holdings"][ticker]["quantity"] * current_price
        account["total_portfolio_value"] = total_value

        # Update account values for metrics
        account_values[current_date] = total_value

        # Update rankings
        rank = update_strategy_ranks(strategies, points, trading_simulator)

        # Log daily results
        logger.info("-------------------------------------------------")
        logger.info(f"Account Cash: ${account['cash']:,.2f}")
        logger.info(f"Trades: {account['trades']}")
        logger.info(f"Holdings: {account['holdings']}")
        logger.info(f"Total Portfolio Value: ${account['total_portfolio_value']:,.2f}")
        logger.info(f"Active Count: {active_count}")
        logger.info("-------------------------------------------------")

        current_date += timedelta(days=1)
        time.sleep(5)

    # Calculate final metrics and generate tear sheet
    metrics = calculate_metrics(account_values)
    logger.info("Final metrics calculated.")
    logger.info(metrics)

    generate_tear_sheet(account_values, metrics)
    logger.info("Tear sheet generated.")

    # Print final results
    logger.info("Testing Completed.")
    logger.info("-------------------------------------------------")
    logger.info(f"Account Cash: ${account['cash']:,.2f}")
    logger.info(f"Total Portfolio Value: ${account['total_portfolio_value']:,.2f}")
    logger.info("-------------------------------------------------")
