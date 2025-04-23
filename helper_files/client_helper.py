import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import yfinance as yf
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from pymongo import MongoClient

from control import stop_loss, take_profit
from strategies.talib_indicators import (
    AD_indicator,
    ADOSC_indicator,
    ADX_indicator,
    ADXR_indicator,
    APO_indicator,
    AROON_indicator,
    AROONOSC_indicator,
    ATR_indicator,
    AVGPRICE_indicator,
    BBANDS_indicator,
    BETA_indicator,
    BOP_indicator,
    CCI_indicator,
    CDL2CROWS_indicator,
    CDL3BLACKCROWS_indicator,
    CDL3INSIDE_indicator,
    CDL3LINESTRIKE_indicator,
    CDL3OUTSIDE_indicator,
    CDL3STARSINSOUTH_indicator,
    CDL3WHITESOLDIERS_indicator,
    CDLABANDONEDBABY_indicator,
    CDLADVANCEBLOCK_indicator,
    CDLBELTHOLD_indicator,
    CDLBREAKAWAY_indicator,
    CDLCLOSINGMARUBOZU_indicator,
    CDLCONCEALBABYSWALL_indicator,
    CDLCOUNTERATTACK_indicator,
    CDLDARKCLOUDCOVER_indicator,
    CDLDOJI_indicator,
    CDLDOJISTAR_indicator,
    CDLDRAGONFLYDOJI_indicator,
    CDLENGULFING_indicator,
    CDLEVENINGDOJISTAR_indicator,
    CDLEVENINGSTAR_indicator,
    CDLGAPSIDESIDEWHITE_indicator,
    CDLGRAVESTONEDOJI_indicator,
    CDLHAMMER_indicator,
    CDLHANGINGMAN_indicator,
    CDLHARAMI_indicator,
    CDLHARAMICROSS_indicator,
    CDLHIGHWAVE_indicator,
    CDLHIKKAKE_indicator,
    CDLHIKKAKEMOD_indicator,
    CDLHOMINGPIGEON_indicator,
    CDLIDENTICAL3CROWS_indicator,
    CDLINNECK_indicator,
    CDLINVERTEDHAMMER_indicator,
    CDLKICKING_indicator,
    CDLKICKINGBYLENGTH_indicator,
    CDLLADDERBOTTOM_indicator,
    CDLLONGLEGGEDDOJI_indicator,
    CDLLONGLINE_indicator,
    CDLMARUBOZU_indicator,
    CDLMATCHINGLOW_indicator,
    CDLMATHOLD_indicator,
    CDLMORNINGDOJISTAR_indicator,
    CDLMORNINGSTAR_indicator,
    CDLONNECK_indicator,
    CDLPIERCING_indicator,
    CDLRICKSHAWMAN_indicator,
    CDLRISEFALL3METHODS_indicator,
    CDLSEPARATINGLINES_indicator,
    CDLSHOOTINGSTAR_indicator,
    CDLSHORTLINE_indicator,
    CDLSPINNINGTOP_indicator,
    CDLSTALLEDPATTERN_indicator,
    CDLSTICKSANDWICH_indicator,
    CDLTAKURI_indicator,
    CDLTASUKIGAP_indicator,
    CDLTHRUSTING_indicator,
    CDLTRISTAR_indicator,
    CDLUNIQUE3RIVER_indicator,
    CDLUPSIDEGAP2CROWS_indicator,
    CDLXSIDEGAP3METHODS_indicator,
    CMO_indicator,
    CORREL_indicator,
    DEMA_indicator,
    DX_indicator,
    EMA_indicator,
    HT_DCPERIOD_indicator,
    HT_DCPHASE_indicator,
    HT_PHASOR_indicator,
    HT_SINE_indicator,
    HT_TRENDLINE_indicator,
    HT_TRENDMODE_indicator,
    KAMA_indicator,
    LINEARREG_ANGLE_indicator,
    LINEARREG_indicator,
    LINEARREG_INTERCEPT_indicator,
    LINEARREG_SLOPE_indicator,
    MA_indicator,
    MACD_indicator,
    MACDEXT_indicator,
    MACDFIX_indicator,
    MAMA_indicator,
    MAVP_indicator,
    MEDPRICE_indicator,
    MFI_indicator,
    MIDPOINT_indicator,
    MIDPRICE_indicator,
    MINUS_DI_indicator,
    MINUS_DM_indicator,
    MOM_indicator,
    NATR_indicator,
    OBV_indicator,
    PLUS_DI_indicator,
    PLUS_DM_indicator,
    PPO_indicator,
    ROC_indicator,
    ROCP_indicator,
    ROCR100_indicator,
    ROCR_indicator,
    RSI_indicator,
    SAR_indicator,
    SAREXT_indicator,
    SMA_indicator,
    STDDEV_indicator,
    STOCH_indicator,
    STOCHF_indicator,
    STOCHRSI_indicator,
    T3_indicator,
    TEMA_indicator,
    TRANGE_indicator,
    TRIMA_indicator,
    TRIX_indicator,
    TSF_indicator,
    TYPPRICE_indicator,
    ULTOSC_indicator,
    VAR_indicator,
    WCLPRICE_indicator,
    WILLR_indicator,
    WMA_indicator,
)

sys.path.append("..")

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))


overlap_studies = [
    BBANDS_indicator,
    DEMA_indicator,
    EMA_indicator,
    HT_TRENDLINE_indicator,
    KAMA_indicator,
    MA_indicator,
    MAMA_indicator,
    MAVP_indicator,
    MIDPOINT_indicator,
    MIDPRICE_indicator,
    SAR_indicator,
    SAREXT_indicator,
    SMA_indicator,
    T3_indicator,
    TEMA_indicator,
    TRIMA_indicator,
    WMA_indicator,
]
momentum_indicators = [
    ADX_indicator,
    ADXR_indicator,
    APO_indicator,
    AROON_indicator,
    AROONOSC_indicator,
    BOP_indicator,
    CCI_indicator,
    CMO_indicator,
    DX_indicator,
    MACD_indicator,
    MACDEXT_indicator,
    MACDFIX_indicator,
    MFI_indicator,
    MINUS_DI_indicator,
    MINUS_DM_indicator,
    MOM_indicator,
    PLUS_DI_indicator,
    PLUS_DM_indicator,
    PPO_indicator,
    ROC_indicator,
    ROCP_indicator,
    ROCR_indicator,
    ROCR100_indicator,
    RSI_indicator,
    STOCH_indicator,
    STOCHF_indicator,
    STOCHRSI_indicator,
    TRIX_indicator,
    ULTOSC_indicator,
    WILLR_indicator,
]
volume_indicators = [AD_indicator, ADOSC_indicator, OBV_indicator]
cycle_indicators = [
    HT_DCPERIOD_indicator,
    HT_DCPHASE_indicator,
    HT_PHASOR_indicator,
    HT_SINE_indicator,
    HT_TRENDMODE_indicator,
]
price_transforms = [
    AVGPRICE_indicator,
    MEDPRICE_indicator,
    TYPPRICE_indicator,
    WCLPRICE_indicator,
]
volatility_indicators = [ATR_indicator, NATR_indicator, TRANGE_indicator]
pattern_recognition = [
    CDL2CROWS_indicator,
    CDL3BLACKCROWS_indicator,
    CDL3INSIDE_indicator,
    CDL3LINESTRIKE_indicator,
    CDL3OUTSIDE_indicator,
    CDL3STARSINSOUTH_indicator,
    CDL3WHITESOLDIERS_indicator,
    CDLABANDONEDBABY_indicator,
    CDLADVANCEBLOCK_indicator,
    CDLBELTHOLD_indicator,
    CDLBREAKAWAY_indicator,
    CDLCLOSINGMARUBOZU_indicator,
    CDLCONCEALBABYSWALL_indicator,
    CDLCOUNTERATTACK_indicator,
    CDLDARKCLOUDCOVER_indicator,
    CDLDOJI_indicator,
    CDLDOJISTAR_indicator,
    CDLDRAGONFLYDOJI_indicator,
    CDLENGULFING_indicator,
    CDLEVENINGDOJISTAR_indicator,
    CDLEVENINGSTAR_indicator,
    CDLGAPSIDESIDEWHITE_indicator,
    CDLGRAVESTONEDOJI_indicator,
    CDLHAMMER_indicator,
    CDLHANGINGMAN_indicator,
    CDLHARAMI_indicator,
    CDLHARAMICROSS_indicator,
    CDLHIGHWAVE_indicator,
    CDLHIKKAKE_indicator,
    CDLHIKKAKEMOD_indicator,
    CDLHOMINGPIGEON_indicator,
    CDLIDENTICAL3CROWS_indicator,
    CDLINNECK_indicator,
    CDLINVERTEDHAMMER_indicator,
    CDLKICKING_indicator,
    CDLKICKINGBYLENGTH_indicator,
    CDLLADDERBOTTOM_indicator,
    CDLLONGLEGGEDDOJI_indicator,
    CDLLONGLINE_indicator,
    CDLMARUBOZU_indicator,
    CDLMATCHINGLOW_indicator,
    CDLMATHOLD_indicator,
    CDLMORNINGDOJISTAR_indicator,
    CDLMORNINGSTAR_indicator,
    CDLONNECK_indicator,
    CDLPIERCING_indicator,
    CDLRICKSHAWMAN_indicator,
    CDLRISEFALL3METHODS_indicator,
    CDLSEPARATINGLINES_indicator,
    CDLSHOOTINGSTAR_indicator,
    CDLSHORTLINE_indicator,
    CDLSPINNINGTOP_indicator,
    CDLSTALLEDPATTERN_indicator,
    CDLSTICKSANDWICH_indicator,
    CDLTAKURI_indicator,
    CDLTASUKIGAP_indicator,
    CDLTHRUSTING_indicator,
    CDLTRISTAR_indicator,
    CDLUNIQUE3RIVER_indicator,
    CDLUPSIDEGAP2CROWS_indicator,
    CDLXSIDEGAP3METHODS_indicator,
]
statistical_functions = [
    BETA_indicator,
    CORREL_indicator,
    LINEARREG_indicator,
    LINEARREG_ANGLE_indicator,
    LINEARREG_INTERCEPT_indicator,
    LINEARREG_SLOPE_indicator,
    STDDEV_indicator,
    TSF_indicator,
    VAR_indicator,
]

strategies = (
    overlap_studies
    + momentum_indicators
    + volume_indicators
    + cycle_indicators
    + price_transforms
    + volatility_indicators
    + pattern_recognition
    + statistical_functions
)


# MongoDB connection helper
def connect_to_mongo(mongo_url):
    """Connect to MongoDB and return the client."""
    return MongoClient(mongo_url)


# Helper to place an order
def place_order(trading_client, symbol, side, quantity, mongo_client):
    """
    Place a market order and log the order to MongoDB only when the order is filled.
    Includes a retry mechanism to periodically check order status.

    :param trading_client: The Alpaca trading client instance
    :param symbol: The stock symbol to trade
    :param side: Order side (OrderSide.BUY or OrderSide.SELL)
    :param qty: Quantity to trade
    :param mongo_client: MongoDB client instance
    :return: Order result from Alpaca API
    """

    market_order_data = MarketOrderRequest(
        symbol=symbol, qty=quantity, side=side, time_in_force=TimeInForce.DAY
    )
    order = trading_client.submit_order(market_order_data)
    order_id = order.id
    qty = round(quantity, 3)
    current_price = get_latest_price(symbol)
    stop_loss_price = round(current_price * (1 - stop_loss), 2)  # 3% loss
    take_profit_price = round(current_price * (1 + take_profit), 2)  # 5% profit

    # Check if order is filled immediately
    order_status = trading_client.get_order_by_id(order_id).status
    
    # If order is not filled immediately, set up a tracker in MongoDB
    if order_status != "filled":
        db = mongo_client.trades
        db.pending_orders.insert_one({
            "order_id": order_id,
            "symbol": symbol,
            "qty": qty,
            "side": side.name,
            "time_in_force": TimeInForce.DAY.name,
            "submitted_at": datetime.now(tz=timezone.utc),
            "status": order_status,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
            "retries": 0,
            "max_retries": 10  # Check up to 10 times
        })
        logging.info(f"Order for {symbol} is {order_status}. Added to pending orders queue.")
        return order
    
    # If order is filled immediately, update MongoDB
    # Log trade details to MongoDB
    db = mongo_client.trades
    db.paper.insert_one(
        {
            "symbol": symbol,
            "qty": qty,
            "side": side.name,
            "time_in_force": TimeInForce.DAY.name,
            "time": datetime.now(tz=timezone.utc),
            "status": "filled",
            "filled": True,
            "order_id": order_id
        }
    )

    # Track assets as well
    assets = db.assets_quantities
    limits = db.assets_limit

    if side == OrderSide.BUY:
        assets.update_one({"symbol": symbol}, {"$inc": {"quantity": qty}}, upsert=True)
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
    elif side == OrderSide.SELL:
        assets.update_one({"symbol": symbol}, {"$inc": {"quantity": -qty}}, upsert=True)
        if assets.find_one({"symbol": symbol})["quantity"] == 0:
            assets.delete_one({"symbol": symbol})
            limits.delete_one({"symbol": symbol})

    return order


def check_pending_orders(trading_client, mongo_client):
    """
    Periodically check pending orders to see if they've been filled.
    If filled, update MongoDB accordingly.
    
    :param trading_client: The Alpaca trading client instance
    :param mongo_client: MongoDB client instance
    """
    db = mongo_client.trades
    pending_orders = list(db.pending_orders.find({"retries": {"$lt": 10}}))
    
    for pending_order in pending_orders:
        try:
            order_id = pending_order["order_id"]
            order = trading_client.get_order_by_id(order_id)
            order_status = order.status
            
            # Update retry count
            db.pending_orders.update_one(
                {"order_id": order_id},
                {"$inc": {"retries": 1}, "$set": {"status": order_status}}
            )
            
            if order_status == "filled":
                # Order has been filled, update MongoDB
                symbol = pending_order["symbol"]
                qty = pending_order["qty"]
                side = pending_order["side"]
                stop_loss_price = pending_order["stop_loss_price"]
                take_profit_price = pending_order["take_profit_price"]
                
                # Log the filled order
                db.paper.insert_one(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "side": side,
                        "time_in_force": pending_order["time_in_force"],
                        "time": datetime.now(tz=timezone.utc),
                        "status": "filled",
                        "filled": True,
                        "order_id": order_id
                    }
                )
                
                # Track assets
                assets = db.assets_quantities
                limits = db.assets_limit
                
                if side == "BUY":
                    assets.update_one({"symbol": symbol}, {"$inc": {"quantity": qty}}, upsert=True)
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
                elif side == "SELL":
                    assets.update_one({"symbol": symbol}, {"$inc": {"quantity": -qty}}, upsert=True)
                    if assets.find_one({"symbol": symbol})["quantity"] == 0:
                        assets.delete_one({"symbol": symbol})
                        limits.delete_one({"symbol": symbol})
                
                # Remove from pending orders
                db.pending_orders.delete_one({"order_id": order_id})
                logging.info(f"Order for {symbol} has been filled and processed.")
            
            elif order_status in ["canceled", "expired", "rejected"]:
                # Order failed, remove from pending orders
                db.pending_orders.delete_one({"order_id": order_id})
                logging.info(f"Order {order_id} for {pending_order['symbol']} has status {order_status}. Removed from pending orders.")
            
            elif pending_order["retries"] >= 9:
                # Max retries reached, log and keep in database with flag
                db.pending_orders.update_one(
                    {"order_id": order_id},
                    {"$set": {"max_retries_reached": True}}
                )
                logging.warning(f"Max retries reached for order {order_id}. Final status: {order_status}")
                
        except Exception as e:
            logging.error(f"Error checking order {pending_order['order_id']}: {e}")


# Helper to retrieve NASDAQ-100 tickers from MongoDB
def get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY):
    """
    Connects to MongoDB and retrieves NASDAQ-100 tickers.

    :param mongo_url: MongoDB connection URL
    :return: List of NASDAQ-100 ticker symbols.
    """

    def call_ndaq_100():
        """
        Fetches the list of NASDAQ 100 tickers using the Financial Modeling Prep API and stores it in a MongoDB collection.
        The MongoDB collection is cleared before inserting the updated list of tickers.
        """
        logging.info("Calling NASDAQ 100 to retrieve tickers.")

        def get_jsonparsed_data(url):
            """
            Parses the JSON response from the provided URL.

            :param url: The API endpoint to retrieve data from.
            :return: Parsed JSON data as a dictionary.
            """
            response = urlopen(url)
            data = response.read().decode("utf-8")
            return json.loads(data)

        try:
            # API URL for fetching NASDAQ 100 tickers
            ndaq_url = f"https://financialmodelingprep.com/api/v3/nasdaq_constituent?apikey={FINANCIAL_PREP_API_KEY}"  # noqa: E231
            ndaq_stocks = get_jsonparsed_data(ndaq_url)
            logging.info("Successfully retrieved NASDAQ 100 tickers.")
        except Exception as e:
            logging.error(f"Error fetching NASDAQ 100 tickers: {e}")
            return
        try:
            # MongoDB connection details

            db = mongo_client.stock_list
            ndaq100_tickers = db.ndaq100_tickers

            ndaq100_tickers.delete_many({})  # Clear existing data
            ndaq100_tickers.insert_many(ndaq_stocks)  # Insert new data
            logging.info("Successfully inserted NASDAQ 100 tickers into MongoDB.")
        except Exception as e:
            logging.error(f"Error inserting tickers into MongoDB: {e}")

    call_ndaq_100()

    tickers = [
        stock["symbol"] for stock in mongo_client.stock_list.ndaq100_tickers.find()
    ]

    return tickers


# Market status checker helper
def market_status(polygon_client):
    """
    Check market status using the Polygon API.

    :param polygon_client: An instance of the Polygon RESTClient
    :return: Current market status ('open', 'early_hours', 'closed')
    """
    try:
        status = polygon_client.get_market_status()
        if status.exchanges.nasdaq == "open" and status.exchanges.nyse == "open":
            return "open"
        elif status.early_hours:
            return "early_hours"
        else:
            return "closed"
    except Exception as e:
        logging.error(f"Error retrieving market status: {e}")
        return "error"


# Helper to get latest price
def get_latest_price(ticker):
    """
    Fetch the latest price for a given stock ticker using yfinance.

    :param ticker: The stock ticker symbol
    :return: The latest price of the stock
    """
    try:
        ticker_yahoo = yf.Ticker(ticker)
        data = ticker_yahoo.history()

        return round(data["Close"].iloc[-1], 2)
    except Exception as e:
        logging.error(f"Error fetching latest price for {ticker}: {e}")
        return None


def dynamic_period_selector(ticker):
    """
    Determines the best period to use for fetching historical data.

    Args:
    - ticker (str): Stock ticker symbol.

    Returns:
    - str: Optimal period for historical data retrieval.
    """
    periods = ["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "ytd", "max"]
    volatility_scores = []

    for period in periods:
        try:
            data = yf.Ticker(ticker).history(period=period)
            if data.empty:
                continue

            # Calculate metrics for decision-making
            daily_changes = data["Close"].pct_change().dropna()
            volatility = daily_changes.std()
            trend_strength = (
                abs(data["Close"].iloc[-1] - data["Close"].iloc[0])
                / data["Close"].iloc[0]
            )

            # Combine metrics into a single score (weight them as desired)
            score = volatility * 0.7 + trend_strength * 0.3
            volatility_scores.append((period, score))
        except Exception as e:
            print(f"Error fetching data for period {period}: {e}")
            continue

    # Select the period with the highest score

    optimal_period = (
        min(volatility_scores, key=lambda x: x[1])[0] if volatility_scores else "1y"
    )
    return optimal_period
