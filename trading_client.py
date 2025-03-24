"""
TODO - When market is closed orders are prefilled but we log them to mongodb. Change this.
Only when orders are filled we need to log them

Phase 2 - Use parallelization for trades with different client ids
"""

import asyncio
import heapq
import logging
import time
from datetime import datetime, timezone
from statistics import median

from ib_insync import IB, MarketOrder, Stock
from pymongo import MongoClient

from config import mongo_url
from control import (  # train_tickers,
    suggestion_heap_limit,
    trade_asset_limit,
    trade_liquidity_limit,
)
from helper_files.client_helper import (
    get_latest_price,
    get_ndaq_tickers,
    market_status,
    strategies,
)
from strategies.talib_indicators import get_data, simulate_strategy

# Global variables
buy_heap = []
suggestion_heap = []
sold = False

# NEED FOR PARALLELIZATION OF TRADES IN FUTURE
# Global counter to generate unique client IDs
# client_id_counter = itertools.count(1)


def get_ib():
    # Create and set a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Create IB instance and connect using a unique clientId
    ib = IB()
    # cid = next(client_id_counter)  # Use a unique client id for each connection
    ib.connect("127.0.0.1", 4002, clientId=1)
    return ib


def weighted_majority_decision_and_median_quantity(decisions_and_quantities):
    """
    Determines the majority decision (buy, sell, or hold) and returns the weighted median quantity.
    """
    buy_decisions = ["buy", "strong buy"]
    sell_decisions = ["sell", "strong sell"]

    weighted_buy_quantities = []
    weighted_sell_quantities = []
    buy_weight = 0
    sell_weight = 0
    hold_weight = 0

    for decision, quantity, weight in decisions_and_quantities:
        if decision in buy_decisions:
            weighted_buy_quantities.append(quantity)
            buy_weight += weight
        elif decision in sell_decisions:
            weighted_sell_quantities.append(quantity)
            sell_weight += weight
        elif decision == "hold":
            hold_weight += weight

    if buy_weight > sell_weight and buy_weight > hold_weight:
        return (
            "buy",
            median(weighted_buy_quantities) if weighted_buy_quantities else 0,
            buy_weight,
            sell_weight,
            hold_weight,
        )
    elif sell_weight > buy_weight and sell_weight > hold_weight:
        return (
            "sell",
            median(weighted_sell_quantities) if weighted_sell_quantities else 0,
            buy_weight,
            sell_weight,
            hold_weight,
        )
    else:
        return "hold", 0, buy_weight, sell_weight, hold_weight


def place_order(ib, symbol, side, quantity, mongo_client):
    """
    Place a market order using IBKR's ib_insync and log the order to MongoDB.

    :param ib: A connected IB instance.
    :param symbol: Stock symbol (e.g., 'AAPL').
    :param side: 'BUY' or 'SELL' (case-insensitive).
    :param quantity: Quantity to trade.
    :param mongo_client: MongoDB client instance.
    :return: Trade object from IBKR.
    """
    # Create a contract for the stock (using SMART routing in USD)
    contract = Stock(symbol, "SMART", "USD")
    order = MarketOrder(side.upper(), quantity)
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)

    # print(trade.orderStatus)
    # Calculate stop-loss and take-profit prices
    stop_loss = 0.03  # 3% loss
    take_profit = 0.05  # 5% profit
    current_price = get_latest_price(symbol)
    stop_loss_price = round(current_price * (1 - stop_loss), 2)
    take_profit_price = round(current_price * (1 + take_profit), 2)

    # Log trade details to MongoDB
    db = mongo_client.trades
    db.paper.insert_one(
        {
            "symbol": symbol,
            "qty": round(quantity, 3),
            "side": side.upper(),
            "order_type": "MARKET",
            "time": datetime.now(tz=timezone.utc),
        }
    )

    # Track assets and their limits
    assets = db.assets_quantities
    limits = db.assets_limit

    if side.upper() == "BUY":
        assets.update_one(
            {"symbol": symbol}, {"$inc": {"quantity": round(quantity, 3)}}, upsert=True
        )
        limits.update_one(
            {"symbol": symbol},
            {
                "$set": {
                    "stop_loss_price": stop_loss_price,
                    "take_profit_price": take_profit_price,
                }
            },
            upsert=True,
        )
    elif side.upper() == "SELL":
        assets.update_one(
            {"symbol": symbol}, {"$inc": {"quantity": -round(quantity, 3)}}, upsert=True
        )
        asset = assets.find_one({"symbol": symbol})
        if asset and asset.get("quantity", 0) == 0:
            assets.delete_one({"symbol": symbol})
            limits.delete_one({"symbol": symbol})

    return trade


def process_ticker(ticker, mongo_client, strategy_to_coefficient):
    """
    Process a single ticker for trading decisions using IB.
    """
    global buy_heap, suggestion_heap, sold

    if sold:
        print("Sold boolean is True. Exiting process_ticker function.")
        return

    try:
        # Fetch current price; retry if necessary
        current_price = None
        while current_price is None:
            try:
                current_price = get_latest_price(ticker)
            except Exception as fetch_error:
                logging.warning(
                    f"Error fetching price for {ticker}. Retrying... {fetch_error}"
                )
                break
        if current_price is None:
            return

        # Connect to IB to retrieve account summary
        ib = get_ib()
        acc_summary = ib.accountSummary()
        portfolio = next(
            (item for item in acc_summary if item.tag == "NetLiquidation"), None
        )
        cash = next(
            (item for item in acc_summary if item.tag == "TotalCashValue"), None
        )

        if not (portfolio and cash):
            logging.error("Could not retrieve account summary from IB")
            ib.disconnect()
            return

        buying_power = float(cash.value)
        portfolio_value = float(portfolio.value)

        # Get current portfolio quantity from MongoDB
        asset_collection = mongo_client.trades.assets_quantities
        limits_collection = mongo_client.trades.assets_limit
        asset_info = asset_collection.find_one({"symbol": ticker})
        portfolio_qty = asset_info["quantity"] if asset_info else 0.0

        # Check stop-loss and take-profit conditions
        limit_info = limits_collection.find_one({"symbol": ticker})
        if limit_info:
            stop_loss_price = limit_info["stop_loss_price"]
            take_profit_price = limit_info["take_profit_price"]
            if current_price <= stop_loss_price or current_price >= take_profit_price:
                sold = True
                print(
                    f"Executing SELL order for {ticker} due to stop-loss or take-profit condition"
                )
                order = place_order(
                    ib,
                    symbol=ticker,
                    side="SELL",
                    quantity=portfolio_qty,
                    mongo_client=mongo_client,
                )
                logging.info(f"Executed SELL order for {ticker}: {order}")
                ib.disconnect()
                return

        # Collect strategy decisions
        decisions_and_quantities = []
        indicator_tb = mongo_client.IndicatorsDatabase
        indicator_collection = indicator_tb.Indicators

        for strategy in strategies:
            historical_data = None
            while historical_data is None:
                try:
                    period = indicator_collection.find_one(
                        {"indicator": strategy.__name__}
                    )
                    historical_data = get_data(
                        ticker, mongo_client, period["ideal_period"]
                    )
                except Exception as fetch_error:
                    logging.warning(
                        f"Error fetching historical data for {ticker}. Retrying... {fetch_error}"
                    )
                    time.sleep(60)
            decision, quantity = simulate_strategy(
                strategy,
                ticker,
                current_price,
                historical_data,
                buying_power,
                portfolio_qty,
                portfolio_value,
            )
            print(
                f"Strategy: {strategy.__name__}, Decision: {decision}, Quantity: {quantity} for {ticker}"
            )
            weight = strategy_to_coefficient[strategy.__name__]
            decisions_and_quantities.append((decision, quantity, weight))

        # Determine overall decision
        decision, quantity, buy_weight, sell_weight, hold_weight = (
            weighted_majority_decision_and_median_quantity(decisions_and_quantities)
        )
        print(
            f"Ticker: {ticker}, Decision: {decision}, Quantity: {quantity}, "
            f"Weights: Buy: {buy_weight}, Sell: {sell_weight}, Hold: {hold_weight}"
        )
        # print(f"""
        # Buying Power:            {buying_power}
        # Trade Liquidity Limit:   {trade_liquidity_limit}
        # Quantity:                {quantity}
        # Portfolio Quantity:      {portfolio_qty}
        # Current Price:           {current_price}
        # Portfolio Value:         {portfolio_value}
        # Trade Asset Limit:      {trade_asset_limit}
        # CALC:                   {((quantity + portfolio_qty) * current_price) / portfolio_value}
        # """)
        # Queue orders based on the decision
        if (
            decision == "buy"
            and buying_power > trade_liquidity_limit
            and (((quantity + portfolio_qty) * current_price) / portfolio_value)
            < trade_asset_limit
        ):
            heapq.heappush(
                buy_heap,
                (-(buy_weight - (sell_weight + (hold_weight * 0.5))), quantity, ticker),
            )
        elif decision == "sell" and portfolio_qty > 0:
            print(f"Executing SELL order for {ticker}")
            sold = True
            quantity = max(quantity, 1)
            order = place_order(
                ib,
                symbol=ticker,
                side="SELL",
                quantity=quantity,
                mongo_client=mongo_client,
            )
            # print(order.log)
            logging.info(f"Executed SELL order for {ticker}: {order}")
        elif (
            portfolio_qty == 0.0
            and buy_weight > sell_weight
            and (((quantity + portfolio_qty) * current_price) / portfolio_value)
            < trade_asset_limit
            and buying_power > trade_liquidity_limit
        ):
            max_investment = portfolio_value * trade_asset_limit
            buy_quantity = min(
                int(max_investment // current_price),
                int(buying_power // current_price),
            )
            if buy_weight > suggestion_heap_limit:
                buy_quantity = max(buy_quantity, 2)
                buy_quantity = buy_quantity // 2
                print(
                    f"Suggestions for buying for {ticker} with a weight of {buy_weight} and quantity of {buy_quantity}"
                )
                heapq.heappush(
                    suggestion_heap,
                    (-(buy_weight - sell_weight), buy_quantity, ticker),
                )
            else:
                logging.info(f"Holding for {ticker}, no action taken.")
        else:
            logging.info(f"Holding for {ticker}, no action taken.")

        ib.disconnect()

    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")


def initialize_strategy_coefficients(mongo_client):
    """
    Initialize strategy coefficients from MongoDB.
    """
    strategy_to_coefficient = {}
    sim_db = mongo_client.trading_simulator
    rank_collection = sim_db.rank
    r_t_c_collection = sim_db.rank_to_coefficient

    for strategy in strategies:
        rank = rank_collection.find_one({"strategy": strategy.__name__})["rank"]
        coefficient = r_t_c_collection.find_one({"rank": rank})["coefficient"]
        strategy_to_coefficient[strategy.__name__] = coefficient

    return strategy_to_coefficient


def update_portfolio_tracking(mongo_client, portfolio_value, qqq_latest, spy_latest):
    """
    Update portfolio performance tracking in MongoDB
    """
    trades_db = mongo_client.trades
    portfolio_collection = trades_db.portfolio_values

    portfolio_collection.update_one(
        {"name": "portfolio_percentage"},
        {"$set": {"portfolio_value": (portfolio_value - 50491.13) / 50491.13}},
    )
    portfolio_collection.update_one(
        {"name": "ndaq_percentage"},
        {"$set": {"portfolio_value": (qqq_latest - 518.58) / 518.58}},
    )
    portfolio_collection.update_one(
        {"name": "spy_percentage"},
        {"$set": {"portfolio_value": (spy_latest - 591.95) / 591.95}},
    )


def process_market_open(mongo_client):
    """
    Handle trading operations when the market is open using IB.
    """
    global buy_heap, suggestion_heap, sold, train_tickers

    ib = get_ib()
    acc_summary = ib.accountSummary()

    portfolio = next(
        (item for item in acc_summary if item.tag == "NetLiquidation"), None
    )
    cash = next((item for item in acc_summary if item.tag == "TotalCashValue"), None)

    if not (portfolio and cash):
        logging.error("Could not retrieve account summary from IB")
        ib.disconnect()
        return

    portfolio_value = float(portfolio.value)
    # buying_power = float(cash.value)

    if not train_tickers:
        train_tickers = get_ndaq_tickers()

    strategy_to_coefficient = initialize_strategy_coefficients(mongo_client)
    qqq_latest = get_latest_price("QQQ")
    spy_latest = get_latest_price("SPY")

    update_portfolio_tracking(mongo_client, portfolio_value, qqq_latest, spy_latest)

    buy_heap = []
    suggestion_heap = []
    sold = False

    # Process each ticker sequentially instead of using threads
    for ticker in train_tickers:
        process_ticker(ticker, mongo_client, strategy_to_coefficient)
        # Add a small delay between processing tickers to avoid overwhelming the API
        time.sleep(0.5)

    execute_buy_orders(mongo_client)
    # print(util.df(ib.positions()))
    print("Sleeping for 30 seconds...")
    time.sleep(30)
    ib.disconnect()


def execute_buy_orders(mongo_client):
    """
    Execute buy orders from the buy and suggestion heaps using IB.
    """
    global buy_heap, suggestion_heap, sold

    ib = get_ib()
    acc_summary = ib.accountSummary()
    portfolio = next(
        (item for item in acc_summary if item.tag == "NetLiquidation"), None
    )
    cash = next((item for item in acc_summary if item.tag == "TotalCashValue"), None)
    if not (portfolio and cash):
        logging.error("Could not retrieve account summary from IB")
        ib.disconnect()
        return
    account_cash = float(cash.value)

    while (
        (buy_heap or suggestion_heap)
        and account_cash > trade_liquidity_limit
        and not sold
    ):
        try:
            acc_summary = ib.accountSummary()
            portfolio = next(
                (item for item in acc_summary if item.tag == "NetLiquidation"), None
            )
            cash = next(
                (item for item in acc_summary if item.tag == "TotalCashValue"), None
            )
            account_cash = float(cash.value)

            if buy_heap and account_cash > trade_liquidity_limit:
                _, quantity, ticker = heapq.heappop(buy_heap)
                print(f"Executing BUY order for {ticker} of quantity {quantity}")
                order = place_order(
                    ib,
                    symbol=ticker,
                    side="BUY",
                    quantity=quantity,
                    mongo_client=mongo_client,
                )
                logging.info(f"Executed BUY order for {ticker}: {order}")
            elif suggestion_heap and account_cash > trade_liquidity_limit:
                _, quantity, ticker = heapq.heappop(suggestion_heap)
                print(f"Executing BUY order for {ticker} of quantity {quantity}")
                order = place_order(
                    ib,
                    symbol=ticker,
                    side="BUY",
                    quantity=quantity,
                    mongo_client=mongo_client,
                )
                logging.info(f"Executed BUY order for {ticker}: {order}")

            time.sleep(5)
        except Exception as e:
            print(f"Error occurred while executing buy order due to {e}. Continuing...")
            break

    buy_heap = []
    suggestion_heap = []
    sold = False
    ib.disconnect()


def process_early_hours():
    """
    Handle operations during market early hours.
    """
    logging.info("Market is in early hours. Waiting for 30 seconds.")
    time.sleep(30)


def process_market_closed():
    """
    Handle operations when the market is closed.
    """
    logging.info("Market is closed. Performing post-market operations.")
    time.sleep(30)


def main():
    """
    Main function to control the trading workflow based on market status.
    """
    logging.info("Trading mode is live.")
    mongo_client = MongoClient(mongo_url)

    while True:
        try:
            status = market_status()
            market_db = mongo_client.market_data
            market_collection = market_db.market_status
            market_collection.update_one({}, {"$set": {"market_status": status}})

            if status == "open":
                process_market_open(mongo_client)
            elif status == "early_hours":
                process_early_hours()
            elif status == "closed":
                process_market_closed()
            else:
                logging.error("An error occurred while checking market status.")
                time.sleep(60)

        except Exception as e:
            logging.error(f"Unexpected error in main trading loop: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
