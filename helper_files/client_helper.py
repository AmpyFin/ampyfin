import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from ib_insync import MarketOrder, Stock
from pymongo import MongoClient

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
# strategies = [
#     MACD_indicator,
#     MACDEXT_indicator,
#     MACDFIX_indicator,
# ]


# MongoDB connection helper
def connect_to_mongo(mongo_url):
    """Connect to MongoDB and return the client."""
    return MongoClient(mongo_url)


def place_order(ib, symbol, side, quantity, mongo_client):
    """
    Place a market order using IBKR's ib_insync and log the order to MongoDB.

    :param ib: A connected IB instance (from ib_insync)
    :param symbol: The stock symbol to trade (e.g., 'AAPL')
    :param side: Order side as a string ('BUY' or 'SELL')
    :param quantity: Quantity to trade (as a float or int)
    :param mongo_client: MongoDB client instance
    :return: Trade object from IBKR API
    """
    # Create a contract for the stock (assumes SMART routing and USD)
    contract = Stock(symbol, "SMART", "USD")

    # Create a market order with the given side and quantity
    order = MarketOrder(side.upper(), quantity)

    # Place the order and retrieve the trade object
    trade = ib.placeOrder(contract, order)

    # Wait a moment for the order to be processed.
    # In production, consider using proper event handling or waiting for a status update.
    ib.sleep(1)

    # Determine the executed price from the trade fills.
    # If multiple fills exist, compute the volume-weighted average price.
    if trade.fills:
        total_shares = sum(fill.execution.shares for fill in trade.fills)
        executed_price = (
            sum(fill.execution.price * fill.execution.shares for fill in trade.fills)
            / total_shares
        )
    else:
        raise Exception("Order was not filled; cannot retrieve execution price.")

    # Define stop loss and take profit percentages (3% loss, 5% profit)
    stop_loss_pct = 0.03
    take_profit_pct = 0.05
    stop_loss_price = round(executed_price * (1 - stop_loss_pct), 2)
    take_profit_price = round(executed_price * (1 + take_profit_pct), 2)

    # Log trade details to MongoDB
    db = mongo_client.trades
    db.paper.insert_one(
        {
            "symbol": symbol,
            "qty": round(quantity, 3),
            "side": side.upper(),
            "order_type": "MARKET",
            "executed_price": executed_price,
            "time": datetime.now(tz=timezone.utc),
        }
    )

    # Track asset quantities and stop/take profit limits in MongoDB
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


def get_ndaq_tickers():
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    tables = pd.read_html(url)
    df = tables[4]  # NASDAQ-100 companies table
    return df["Ticker"].tolist()


def market_status():
    nasdaq = mcal.get_calendar("NASDAQ")
    now = pd.Timestamp.utcnow()

    schedule = nasdaq.schedule(start_date=now.date(), end_date=now.date())

    if schedule.empty:
        return "closed"

    market_open = schedule.iloc[0]["market_open"]
    market_close = schedule.iloc[0]["market_close"]

    if market_open <= now <= market_close:
        return "open"
    elif now < market_open:
        return "pre_market"
    else:
        return "closed"


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
