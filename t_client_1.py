'''
MODIFICATIONS - Changed how indicators are loaded from MongoDB
'''

import asyncio
import heapq
import logging
import time
from datetime import datetime, timezone

from ib_insync import IB, MarketOrder, Stock, AccountValue
from pymongo import MongoClient
from statistics import median
from config import mongo_url
from control import (
    suggestion_heap_limit,
    trade_asset_limit,
    trade_liquidity_limit,
    train_tickers
)
from helper_files.client_helper import (
    get_latest_price,
    get_ndaq_tickers,
    market_status,
    strategies,
)
from strategies.talib_indicators import get_data, simulate_strategy

# ─── Global cache for account values ──────────────────────────────────────────
latest_cash_value = 0.0
latest_portfolio_value = 0.0


def account_summary_handler(accountValue: AccountValue):
    """
    This will fire every time IB pushes a new accountValue.
    We only care about TotalCashValue and NetLiquidation.
    """
    global latest_cash_value, latest_portfolio_value

    if accountValue.tag == "TotalCashValue":
        latest_cash_value = float(accountValue.value)
        print(f"[AccountSummary] TotalCashValue updated → ${latest_cash_value:,.2f}")
    elif accountValue.tag == "NetLiquidation":
        latest_portfolio_value = float(accountValue.value)
        print(f"[AccountSummary] NetLiquidation updated → ${latest_portfolio_value:,.2f}")


def load_indicator_periods(mongo_client):
    coll = mongo_client.IndicatorsDatabase.Indicators
    # one query for all docs
    return {
        doc["indicator"]: doc["ideal_period"]
        for doc in coll.find({}, {"indicator": 1, "ideal_period": 1})
    }


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
    contract = Stock(symbol, "SMART", "USD")
    order = MarketOrder(side.upper(), quantity)
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)  # allows IB to process the order

    # Log to Mongo…
    db = mongo_client.trades
    db.paper.insert_one({
        "symbol": symbol,
        "qty": round(quantity, 3),
        "side": side.upper(),
        "order_type": "MARKET",
        "time": datetime.now(tz=timezone.utc),
    })

    # update assets & limits…
    stop_loss = 0.03  # 3% loss
    take_profit = 0.05  # 5% profit
    current_price = get_latest_price(symbol)
    stop_loss_price = round(current_price * (1 - stop_loss), 2)
    take_profit_price = round(current_price * (1 + take_profit), 2)

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


def process_ticker(ticker, mongo_client, indicator_periods, strategy_to_coefficient, ib):
    global buy_heap, suggestion_heap, sold

    if sold:
        return

    # 1) fetch price
    try:
        current_price = get_latest_price(ticker)
    except Exception:
        logging.warning(f"Price fetch failed for {ticker}")
        return

    # 2) check stop-loss/take-profit using the cached limits…
    asset_collection = mongo_client.trades.assets_quantities
    limits_collection = mongo_client.trades.assets_limit
    asset_info = asset_collection.find_one({"symbol": ticker})
    portfolio_qty = asset_info["quantity"] if asset_info else 0.0

    limit_info = limits_collection.find_one({"symbol": ticker})
    if limit_info:
        stop_loss_price = limit_info["stop_loss_price"]
        take_profit_price = limit_info["take_profit_price"]
        if current_price <= stop_loss_price or current_price >= take_profit_price:
            sold = True
            print(
                f"Executing SELL order for {ticker} due to stop-loss or take-profit condition"
            )
            place_order(
                ib,
                symbol=ticker,
                side="SELL",
                quantity=portfolio_qty,
                mongo_client=mongo_client,
            )
            return

    # 3) gather strategy decisions…
    decisions_and_quantities = []

    for strategy in strategies:
        historical_data = None
        while historical_data is None:
            try:
                period = indicator_periods[strategy.__name__]
                # Fetch historical data for the strategy
                historical_data = get_data(
                    ticker, mongo_client, period
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
            latest_cash_value,
            portfolio_qty,
            latest_portfolio_value,
        )
        print(
            f"Strategy: {strategy.__name__}, Decision: {decision}, Quantity: {quantity} for {ticker}"
        )
        weight = strategy_to_coefficient[strategy.__name__]
        decisions_and_quantities.append((decision, quantity, weight))

    # 4) weighted majority decision…
    decision, quantity, buy_w, sell_w, hold_w = weighted_majority_decision_and_median_quantity(decisions_and_quantities)
    # print('Decisions and Quantities:', decisions_and_quantities)
    # print(decision, quantity, buy_w, sell_w, hold_w)
    # 5) enqueue sell immediately or push buy into heap…
    if decision == "sell" and portfolio_qty > 0:
        sold = True
        place_order(ib, ticker, "SELL", max(quantity, 1), mongo_client)
    elif (decision == "buy"
        and latest_cash_value > trade_liquidity_limit
        and (((quantity + portfolio_qty) * current_price) / latest_portfolio_value) < trade_asset_limit):
        heapq.heappush(buy_heap, (-(buy_w - (sell_w + hold_w*0.5)), quantity, ticker))
    elif (
            portfolio_qty == 0.0
            and buy_w > sell_w
            and (((quantity + portfolio_qty) * current_price) / latest_portfolio_value)
            < trade_asset_limit
            and latest_cash_value > trade_liquidity_limit
        ):
            max_investment = latest_portfolio_value * trade_asset_limit
            buy_quantity = min(
                int(max_investment // current_price),
                int(latest_cash_value // current_price),
            )
            if buy_w > suggestion_heap_limit:
                buy_quantity = max(buy_quantity, 2)
                buy_quantity = buy_quantity // 2
                print(
                    f"Suggestions for buying for {ticker} with a weight of {buy_w} and quantity of {buy_quantity}"
                )
                heapq.heappush(
                    suggestion_heap,
                    (-(buy_w - sell_w), buy_quantity, ticker),
                )
            else:
                logging.info(f"Holding for {ticker}, no action taken.")
    else:
        logging.info(f"Holding for {ticker}, no action taken.")


def process_market_open(mongo_client, ib):
    global buy_heap, suggestion_heap, sold, train_tickers

    if not train_tickers:
        train_tickers = get_ndaq_tickers()
    
    indicator_periods = load_indicator_periods(mongo_client)

    strategy_to_coefficient = initialize_strategy_coefficients(mongo_client)

    buy_heap = []
    suggestion_heap = []
    sold = False

    for ticker in train_tickers:
        process_ticker(ticker, mongo_client, indicator_periods, strategy_to_coefficient, ib)
        time.sleep(0.5)

    execute_buy_orders(mongo_client, ib)


def execute_buy_orders(mongo_client, ib):
    global buy_heap, suggestion_heap, sold
    print('In execute_buy_orders')
    print(f"Buy heap: {buy_heap}")
    while (buy_heap or suggestion_heap) and latest_cash_value > trade_liquidity_limit and not sold:
        if buy_heap and latest_cash_value > trade_liquidity_limit:
            _, qty, ticker = heapq.heappop(buy_heap)
            place_order(ib, ticker, "BUY", qty, mongo_client)
        elif suggestion_heap and latest_cash_value > trade_liquidity_limit:
            _, qty, ticker = heapq.heappop(suggestion_heap)
            place_order(ib, ticker, "BUY", qty, mongo_client)
        
        time.sleep(5)

    buy_heap = []
    suggestion_heap = []
    sold = False


def main():
    logging.info("Starting trading bot…")
    mongo_client = MongoClient(mongo_url)

    # 1) establish single IB connection
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=1)

    # 2) subscribe to all account summary updates
    ib.accountSummaryEvent += account_summary_handler
    account_summary = ib.accountSummary()  # blocking on first call

    # Print initial values
    print(f"Initial TotalCashValue → ${latest_cash_value:,.2f}")
    print(f"Initial NetLiquidation → ${latest_portfolio_value:,.2f}")
    print('**' * 20)

    # 3) run trading loop
    while True:
        try:
            status = market_status()
            mongo_client.market_data.market_status.update_one({}, {"$set": {"market_status": status}})
            if status == "open":
                process_market_open(mongo_client, ib)
            else:
                time.sleep(60)
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(60)

    # 4) on shutdown
    ib.disconnect()

if __name__ == "__main__":
    main()
