from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pywt  # PyWavelets for wavelet analysis
import scipy
import scipy.spatial
import scipy.stats
import stats
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# 12/7/2024 talib is now available so please use those functions. This file is no longer in use


# Function to fetch historical bar data using Alpaca StockHistoricalDataClient
def get_historical_data(ticker, client, days=100):
    """
    Fetch historical bar data for a given stock ticker.

    :param ticker: The stock ticker symbol.
    :param client: An instance of StockHistoricalDataClient.
    :param days: Number of days of historical data to fetch.
    :return: DataFrame with historical stock bar data.
    """
    start_time = datetime.now() - timedelta(
        days=days
    )  # Get data for the past 'days' days
    request_params = StockBarsRequest(
        symbol_or_symbols=ticker, timeframe=TimeFrame.Day, start=start_time
    )

    bars = client.get_stock_bars(request_params)
    data = bars.df  # Returns a pandas DataFrame
    return data


def rsi_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    RSI strategy: Buy when RSI is oversold (<30), sell when overbought (>70).
    """
    window = 14
    max_investment = total_portfolio_value * 0.10  # 10% of portfolio value to invest

    # Calculate RSI
    delta = historical_data["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(window=window).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    current_rsi = rsi.iloc[-1]

    # Overbought condition (sell/strong sell)
    if current_rsi >= 70:
        if portfolio_qty > 0:
            if current_rsi >= 80:  # Strong sell
                quantity_to_sell = portfolio_qty  # Sell all
                return ("strong sell", quantity_to_sell, ticker)
            else:  # Regular sell
                quantity_to_sell = max(1, int(portfolio_qty * 0.5))  # Sell half
                return ("sell", quantity_to_sell, ticker)

    # Oversold condition (buy/strong buy)
    elif current_rsi <= 30:
        if account_cash > 0:
            if current_rsi <= 20:  # Strong buy
                quantity_to_buy = min(
                    int((max_investment * 1.5) // current_price),
                    int(account_cash // current_price),
                )
                if quantity_to_buy > 0:
                    return ("strong buy", quantity_to_buy, ticker)
            else:  # Regular buy
                quantity_to_buy = min(
                    int(max_investment // current_price),
                    int(account_cash // current_price),
                )
                if quantity_to_buy > 0:
                    return ("buy", quantity_to_buy, ticker)

    # Hold condition
    return ("hold", 0, ticker)


def bollinger_bands_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Bollinger Bands strategy with six bands.
    """
    max_investment = total_portfolio_value * 0.10
    window = 20
    num_std = 2
    # Calculate Bollinger Bands
    historical_data["MA"] = historical_data["close"].rolling(window=window).mean()
    historical_data["STD"] = historical_data["close"].rolling(window=window).std()
    historical_data["Upper_2"] = historical_data["MA"] + (
        num_std * historical_data["STD"]
    )
    historical_data["Lower_2"] = historical_data["MA"] - (
        num_std * historical_data["STD"]
    )
    historical_data["Upper_1"] = historical_data["MA"] + historical_data["STD"]
    historical_data["Lower_1"] = historical_data["MA"] - historical_data["STD"]
    # Get the latest values
    # ma = historical_data["MA"].iloc[-1]
    upper_2 = historical_data["Upper_2"].iloc[-1]
    lower_2 = historical_data["Lower_2"].iloc[-1]
    upper_1 = historical_data["Upper_1"].iloc[-1]
    lower_1 = historical_data["Lower_1"].iloc[-1]

    # Determine sentiment and quantity
    # Determine sentiment and quantity
    if current_price <= lower_2:
        sentiment = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif lower_2 < current_price <= lower_1:
        sentiment = "buy"
        quantity = min(
            int((max_investment * 0.5) // current_price),
            int(account_cash // current_price),
        )
    elif upper_1 <= current_price < upper_2:
        # Check if we have portfolio quantity greater than 0 to sell
        if portfolio_qty > 0:
            sentiment = "sell"
            quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        else:
            sentiment = "hold"
            quantity = 0
    elif current_price >= upper_2:
        # Check if we have portfolio quantity greater than 0 to strong sell
        if portfolio_qty > 0:
            sentiment = "strong sell"
            quantity = portfolio_qty
        else:
            sentiment = "hold"
            quantity = 0
    else:
        sentiment = "hold"
        quantity = 0

    return (sentiment, quantity, ticker)


def momentum_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Momentum strategy logic to determine buy or sell signals based on short and long moving averages.
    Limits the amount to invest to less than 10% of the total portfolio.
    """
    # Maximum percentage of portfolio to invest per trade
    max_investment_percentage = 0.10  # 10% of total portfolio value
    max_investment = total_portfolio_value * max_investment_percentage

    # Momentum Logic
    short_window = 10
    long_window = 50

    short_ma = historical_data["close"].rolling(short_window).mean().iloc[-1]
    long_ma = historical_data["close"].rolling(long_window).mean().iloc[-1]

    # Calculate sentiment
    if short_ma > long_ma:
        sentiment = min(100, int((short_ma / long_ma - 1) * 1000))
    else:
        sentiment = max(1, int((short_ma / long_ma - 1) * 1000))

    # Strong Buy signal (sentiment 81-100)
    if sentiment >= 81 and account_cash > 0:
        amount_to_invest = min(account_cash, max_investment)
        quantity_to_buy = int(amount_to_invest // current_price)
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Buy signal (sentiment 61-80)
    elif 61 <= sentiment <= 80 and account_cash > 0:
        amount_to_invest = (
            min(account_cash, max_investment) * 0.5
        )  # Invest half for regular buy
        quantity_to_buy = int(amount_to_invest // current_price)
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Strong Sell signal (sentiment 1-20)
    elif sentiment <= 20 and portfolio_qty > 0:
        quantity_to_sell = portfolio_qty
        return ("strong sell", quantity_to_sell, ticker)

    # Sell signal (sentiment 21-40)
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))
        return ("sell", quantity_to_sell, ticker)

    # Hold signal (sentiment 41-60)
    return ("hold", 0, ticker)


def mean_reversion_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Mean reversion strategy: Buy if the stock price is below the moving average, sell if above.

    :param ticker: The stock ticker symbol.
    :param current_price: The current price of the stock.
    :param historical_data: Historical stock data for the ticker.
    :param account_cash: Available cash in the account.
    :param portfolio_qty: Quantity of stock held in the portfolio.
    :param total_portfolio_value: Total value of the portfolio.
    :return: Tuple (action, quantity, ticker).
    """

    # Define max investment (10% of total portfolio value)
    max_investment = total_portfolio_value * 0.10

    # Calculate moving average
    window = 20  # Use a 20-day moving average
    historical_data["MA20"] = historical_data["close"].rolling(window=window).mean()

    # Drop NaN values after creating the moving average
    historical_data.dropna(inplace=True)

    # Calculate sentiment
    ma_current = historical_data["MA20"].iloc[-1]
    percent_diff = (current_price - ma_current) / ma_current * 100
    sentiment = int(
        50 - percent_diff
    )  # Invert the scale so below MA is higher sentiment

    # Strong Buy signal (sentiment 81-100)
    if sentiment >= 81 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Buy signal (sentiment 61-80)
    elif 61 <= sentiment <= 80 and account_cash > 0:
        quantity_to_buy = min(
            int((max_investment * 0.5) // current_price),
            int(account_cash // current_price),
        )
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Strong Sell signal (sentiment 1-20)
    elif sentiment <= 20 and portfolio_qty > 0:
        return ("strong sell", portfolio_qty, ticker)

    # Sell signal (sentiment 21-40)
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))
        return ("sell", quantity_to_sell, ticker)

    # Hold signal (sentiment 41-60)
    return ("hold", 0, ticker)


def triple_moving_average_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Triple Moving Average Crossover Strategy: Uses 3 moving averages to generate stronger signals
    """
    max_investment = total_portfolio_value * 0.10

    # Calculate three moving averages
    short_ma = historical_data["close"].rolling(window=5).mean()
    medium_ma = historical_data["close"].rolling(window=20).mean()
    long_ma = historical_data["close"].rolling(window=50).mean()

    # Get current and previous values
    current_short = short_ma.iloc[-1]
    current_medium = medium_ma.iloc[-1]
    current_long = long_ma.iloc[-1]

    prev_short = short_ma.iloc[-2]
    prev_medium = medium_ma.iloc[-2]

    # Calculate sentiment score
    if current_short > current_medium and current_short > current_long:
        if prev_short <= prev_medium:
            sentiment = 90  # Strong buy
        else:
            sentiment = 70  # Buy
    elif current_short < current_medium and current_short < current_long:
        if prev_short >= prev_medium:
            sentiment = 10  # Strong sell
        else:
            sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 41 <= sentiment <= 60:
        action = "hold"
        quantity = 0
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def volume_price_trend_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Volume Price Trend (VPT) Strategy: Combines price and volume for stronger signals
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    # Calculate VPT
    price_change = historical_data["close"].pct_change()
    vpt = (price_change * historical_data["volume"]).cumsum()

    # Calculate VPT moving average
    vpt_ma = vpt.rolling(window=15).mean()

    current_vpt = vpt.iloc[-1]
    prev_vpt = vpt.iloc[-2]
    current_vpt_ma = vpt_ma.iloc[-1]
    prev_vpt_ma = vpt_ma.iloc[-2]

    # Calculate sentiment score
    if prev_vpt <= prev_vpt_ma and current_vpt > current_vpt_ma:
        sentiment = 90  # Strong buy
    elif current_vpt > current_vpt_ma:
        sentiment = 70  # Buy
    elif prev_vpt >= prev_vpt_ma and current_vpt < current_vpt_ma:
        sentiment = 10  # Strong sell
    elif current_vpt < current_vpt_ma:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )

    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0
    return action, quantity, ticker


def keltner_channel_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Keltner Channel Strategy: Similar to Bollinger Bands but uses ATR for volatility
    """
    max_investment = total_portfolio_value * 0.10
    window = 20
    multiplier = 2

    # Calculate ATR
    high_low = historical_data["high"] - historical_data["low"]
    high_close = abs(historical_data["high"] - historical_data["close"].shift())
    low_close = abs(historical_data["low"] - historical_data["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.rolling(window=window).mean()

    # Calculate Keltner Channels
    middle_line = historical_data["close"].rolling(window=window).mean()
    upper_channel = middle_line + (multiplier * atr)
    lower_channel = middle_line - (multiplier * atr)

    # Calculate sentiment score
    if current_price <= lower_channel.iloc[-1]:
        sentiment = 90  # Strong buy
    elif (
        current_price > lower_channel.iloc[-1] and current_price < middle_line.iloc[-1]
    ):
        sentiment = 70  # Buy
    elif current_price >= upper_channel.iloc[-1]:
        sentiment = 10  # Strong sell
    elif (
        current_price < upper_channel.iloc[-1] and current_price > middle_line.iloc[-1]
    ):
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def dual_thrust_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Dual Thrust Strategy: Range breakout strategy with dynamic thresholds
    """
    max_investment = total_portfolio_value * 0.10
    lookback = 4
    k1 = 0.7  # Upper threshold multiplier
    k2 = 0.7  # Lower threshold multiplier

    # Calculate range
    hh = historical_data["high"].rolling(window=lookback).max()
    lc = historical_data["close"].rolling(window=lookback).min()
    hc = historical_data["close"].rolling(window=lookback).max()
    ll = historical_data["low"].rolling(window=lookback).min()
    range_val = pd.concat([hh - lc, hc - ll], axis=1).max(axis=1)

    # Calculate upper and lower bounds
    upper_bound = historical_data["open"].iloc[-1] + k1 * range_val.iloc[-1]
    lower_bound = historical_data["open"].iloc[-1] - k2 * range_val.iloc[-1]

    # Calculate sentiment score
    if current_price > upper_bound:
        sentiment = 80 + min(20, int((current_price - upper_bound) / upper_bound * 100))
    elif current_price < lower_bound:
        sentiment = 20 - min(20, int((lower_bound - current_price) / lower_bound * 100))
    else:
        sentiment = 50 + int(
            (current_price - lower_bound) / (upper_bound - lower_bound) * 20
        )

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def adaptive_momentum_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Adaptive Momentum Strategy using Dynamic Time Warping
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def calculate_adaptive_momentum(data):
        # Dynamic time warping distance between recent and historical patterns
        recent = data["close"].iloc[-5:].values
        historical = data["close"].iloc[-window:-5].values

        # Calculate pattern similarity
        distances = []
        for i in range(len(historical) - len(recent) + 1):
            pattern = historical[i : i + len(recent)]
            dist = np.sum((recent - pattern) ** 2)
            distances.append(dist)

        return np.mean(distances)

    momentum_signal = pd.Series(index=historical_data.index)
    for i in range(window, len(historical_data)):
        momentum_signal.iloc[i] = calculate_adaptive_momentum(
            historical_data.iloc[i - window : i]
        )

    # Calculate sentiment score
    if momentum_signal.iloc[-1] < momentum_signal.mean():
        sentiment = 80 + min(
            20,
            int(
                (momentum_signal.mean() - momentum_signal.iloc[-1])
                / momentum_signal.std()
                * 20
            ),
        )
    else:
        sentiment = 20 - min(
            20,
            int(
                (momentum_signal.iloc[-1] - momentum_signal.mean())
                / momentum_signal.std()
                * 20
            ),
        )

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def hull_moving_average_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Hull Moving Average Strategy: Reduces lag in moving averages
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Hull MA
    def hull_moving_average(data, period):
        half_period = int(period / 2)
        sqrt_period = int(np.sqrt(period))

        wma1 = data.rolling(window=half_period).mean()
        wma2 = data.rolling(window=period).mean()

        hull = (2 * wma1 - wma2).rolling(window=sqrt_period).mean()
        return hull

    prices = historical_data["close"]
    hma = hull_moving_average(prices, window)

    # Calculate sentiment score
    if current_price > hma.iloc[-1] and hma.iloc[-1] > hma.iloc[-2]:
        sentiment = 90  # Strong buy
    elif current_price > hma.iloc[-1]:
        sentiment = 70  # Buy
    elif current_price < hma.iloc[-1] and hma.iloc[-1] < hma.iloc[-2]:
        sentiment = 10  # Strong sell
    elif current_price < hma.iloc[-1]:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def elder_ray_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Elder Ray Strategy: Uses Bull and Bear Power indicators
    """
    max_investment = total_portfolio_value * 0.10
    period = 13

    # Calculate EMA
    ema = historical_data["close"].ewm(span=period, adjust=False).mean()

    # Calculate Bull and Bear Power
    bull_power = historical_data["high"] - ema
    bear_power = historical_data["low"] - ema

    # Calculate sentiment score
    bull_strength = bull_power.iloc[-1] / bull_power.std()
    bear_strength = bear_power.iloc[-1] / bear_power.std()
    sentiment_score = 50 + (bull_strength - bear_strength) * 10
    sentiment_score = max(1, min(100, sentiment_score))  # Clamp between 1 and 100

    # Determine action and quantity based on sentiment score
    if sentiment_score >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment_score >= 61:
        action = "buy"
        quantity = min(
            int((max_investment * 0.5) // current_price),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment_score <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment_score <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def chande_momentum_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Chande Momentum Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 20

    # Calculate price changes
    price_changes = historical_data["close"].diff()

    # Calculate sum of up and down moves
    up_sum = price_changes.rolling(window=period).apply(lambda x: x[x > 0].sum())
    down_sum = price_changes.rolling(window=period).apply(lambda x: abs(x[x < 0].sum()))

    # Calculate CMO
    cmo = 100 * ((up_sum - down_sum) / (up_sum + down_sum))

    # Calculate sentiment score (1-100)
    sentiment_score = int(50 + cmo.iloc[-1] / 2)
    sentiment_score = max(
        1, min(100, sentiment_score)
    )  # Ensure score is between 1 and 100

    # Determine action and quantity based on sentiment score
    if sentiment_score >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment_score <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment_score <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment_score <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def dema_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Double Exponential Moving Average Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 20

    # Calculate DEMA
    ema = historical_data["close"].ewm(span=period, adjust=False).mean()
    ema_of_ema = ema.ewm(span=period, adjust=False).mean()
    dema = 2 * ema - ema_of_ema

    # Calculate sentiment score
    if current_price > dema.iloc[-1] and dema.iloc[-1] > dema.iloc[-2]:
        sentiment = 80 + min(20, (current_price - dema.iloc[-1]) / dema.iloc[-1] * 100)
    elif current_price < dema.iloc[-1] and dema.iloc[-1] < dema.iloc[-2]:
        sentiment = 20 - min(20, (dema.iloc[-1] - current_price) / dema.iloc[-1] * 100)
    else:
        sentiment = 50

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price * 0.5),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def price_channel_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Price Channel Breakout Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 20

    upper_channel = historical_data["high"].rolling(window=period).max()
    lower_channel = historical_data["low"].rolling(window=period).min()

    if current_price > upper_channel.iloc[-2]:
        # sentiment = 90  # Strong buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "strong buy", quantity, ticker

    elif current_price < lower_channel.iloc[-2] and portfolio_qty > 0:
        # sentiment = 10  # Strong sell
        quantity = portfolio_qty
        return "strong sell", quantity, ticker

    else:
        mid_channel = (upper_channel.iloc[-2] + lower_channel.iloc[-2]) / 2
        if current_price > mid_channel:
            # sentiment = 70  # Buy
            quantity = min(
                int(max_investment // current_price), int(account_cash // current_price)
            )
            return "buy", quantity, ticker
        elif current_price < mid_channel and portfolio_qty > 0:
            # sentiment = 30  # Sell
            quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
            return "sell", quantity, ticker
        else:
            # sentiment = 50  # Hold
            return "hold", 0, ticker


def mass_index_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Mass Index Strategy for reversal detection
    """
    max_investment = total_portfolio_value * 0.10
    period = 25

    # Calculate Mass Index
    high_low = historical_data["high"] - historical_data["low"]
    ema1 = high_low.ewm(span=9).mean()
    ema2 = ema1.ewm(span=9).mean()
    ratio = ema1 / ema2
    mass_index = ratio.rolling(window=period).sum()

    if mass_index.iloc[-1] > 27 and mass_index.iloc[-2] < 27:
        # sentiment = 85  # Strong buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "strong buy", quantity, ticker

    elif (
        mass_index.iloc[-1] < 26.5 and mass_index.iloc[-2] > 26.5 and portfolio_qty > 0
    ):
        # sentiment = 15  # Strong sell
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        return "strong sell", quantity, ticker

    else:
        if mass_index.iloc[-1] > 26.75:
            # sentiment = 75  # Buy
            quantity = min(
                int(max_investment * 0.25 // current_price),
                int(account_cash // current_price),
            )
            return "buy", quantity, ticker
        elif mass_index.iloc[-1] < 26.75 and portfolio_qty > 0:
            # sentiment = 35  # Sell
            quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.25)))
            return "sell", quantity, ticker
        else:
            # sentiment = 50  # Hold
            return "hold", 0, ticker


def vortex_indicator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Vortex Indicator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def calculate_vortex_indicator(data):
        high = data["high"]
        low = data["low"]
        close = data["close"]

        tr = np.maximum(
            high - low, np.abs(high - close.shift(1)), np.abs(low - close.shift(1))
        )
        vm_plus = np.abs(high - low.shift(1))
        vm_minus = np.abs(low - high.shift(1))

        vi_plus = vm_plus.rolling(window=window).sum() / tr.rolling(window=window).sum()
        vi_minus = (
            vm_minus.rolling(window=window).sum() / tr.rolling(window=window).sum()
        )

        return vi_plus, vi_minus

    vi_plus, vi_minus = calculate_vortex_indicator(historical_data)

    # Calculate sentiment score
    sentiment = 50 + (vi_plus.iloc[-1] - vi_minus.iloc[-1]) * 100
    sentiment = max(1, min(100, sentiment))  # Ensure sentiment is between 1 and 100

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int(max_investment // current_price * 0.5),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def aroon_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Aroon Indicator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 25

    # Calculate Aroon indicators
    rolling_high = historical_data["high"].rolling(period + 1)
    rolling_low = historical_data["low"].rolling(period + 1)

    aroon_up = rolling_high.apply(lambda x: float(np.argmax(x)) / period * 100)
    aroon_down = rolling_low.apply(lambda x: float(np.argmin(x)) / period * 100)

    # Calculate sentiment score
    aroon_diff = aroon_up.iloc[-1] - aroon_down.iloc[-1]
    sentiment_score = int(50 + aroon_diff / 2)  # Scale to 0-100
    sentiment_score = max(1, min(100, sentiment_score))  # Clamp to 1-100 range

    # Determine action and quantity
    if sentiment_score > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment_score > 60:
        action = "buy"
        quantity = min(
            int(max_investment // current_price * 0.5),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment_score <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment_score <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def ultimate_oscillator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Ultimate Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    def calculate_ultimate_oscillator(data):
        # Calculate buying pressure and true range
        bp = data["close"] - pd.concat(
            [data["low"], data["close"].shift(1)], axis=1
        ).min(axis=1)
        tr = pd.concat(
            [
                data["high"] - data["low"],
                abs(data["high"] - data["close"].shift(1)),
                abs(data["low"] - data["close"].shift(1)),
            ],
            axis=1,
        ).max(axis=1)

        # Calculate averages for different periods
        avg7 = bp.rolling(7).sum() / tr.rolling(7).sum()
        avg14 = bp.rolling(14).sum() / tr.rolling(14).sum()
        avg28 = bp.rolling(28).sum() / tr.rolling(28).sum()

        # Calculate Ultimate Oscillator
        uo = 100 * ((4 * avg7 + 2 * avg14 + avg28) / (4 + 2 + 1))

        return uo.iloc[-1]

    uo = calculate_ultimate_oscillator(historical_data)

    # Calculate sentiment score
    if uo < 30:
        sentiment = int(20 + (uo / 30) * 20)  # 20-40
    elif uo > 70:
        sentiment = int(80 + ((uo - 70) / 30) * 20)  # 80-100
    else:
        sentiment = int(40 + ((uo - 30) / 40) * 40)  # 40-80

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def trix_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    TRIX Strategy: Triple Exponential Average
    """
    max_investment = total_portfolio_value * 0.10
    period = 15
    signal_period = 9

    # Calculate TRIX
    ema1 = historical_data["close"].ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    ema3 = ema2.ewm(span=period, adjust=False).mean()
    trix = 100 * (ema3 - ema3.shift(1)) / ema3.shift(1)
    signal = trix.rolling(window=signal_period).mean()

    # Calculate sentiment score
    diff = trix.iloc[-1] - signal.iloc[-1]
    max_diff = (
        trix.rolling(window=20).max().iloc[-1] - trix.rolling(window=20).min().iloc[-1]
    )
    sentiment = int(50 + (diff / max_diff) * 50)
    sentiment = max(1, min(100, sentiment))

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def kst_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Know Sure Thing (KST) Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    # ROC periods
    r1, r2, r3, r4 = 10, 15, 20, 30
    # SMA periods
    s1, s2, s3, s4 = 10, 10, 10, 15

    # Calculate ROC values
    roc1 = historical_data["close"].diff(r1) / historical_data["close"].shift(r1) * 100
    roc2 = historical_data["close"].diff(r2) / historical_data["close"].shift(r2) * 100
    roc3 = historical_data["close"].diff(r3) / historical_data["close"].shift(r3) * 100
    roc4 = historical_data["close"].diff(r4) / historical_data["close"].shift(r4) * 100

    # Calculate KST
    k1 = roc1.rolling(s1).mean()
    k2 = roc2.rolling(s2).mean()
    k3 = roc3.rolling(s3).mean()
    k4 = roc4.rolling(s4).mean()

    kst = (k1 * 1) + (k2 * 2) + (k3 * 3) + (k4 * 4)
    signal = kst.rolling(9).mean()

    # Calculate sentiment score
    sentiment = 50 + (kst.iloc[-1] - signal.iloc[-1]) * 10
    sentiment = max(1, min(100, sentiment))  # Ensure sentiment is between 1 and 100

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def psar_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Parabolic SAR Strategy
    """
    max_investment = total_portfolio_value * 0.10
    af = 0.02  # Acceleration Factor
    max_af = 0.2

    high = historical_data["high"].values
    low = historical_data["low"].values

    # Initialize arrays
    psar = np.zeros(len(high))
    trend = np.zeros(len(high))
    ep = np.zeros(len(high))
    af_values = np.zeros(len(high))

    # Set initial values
    trend[0] = 1 if high[0] > low[0] else -1
    ep[0] = high[0] if trend[0] == 1 else low[0]
    psar[0] = low[0] if trend[0] == 1 else high[0]
    af_values[0] = af

    # Calculate PSAR
    for i in range(1, len(high)):
        psar[i] = psar[i - 1] + af_values[i - 1] * (ep[i - 1] - psar[i - 1])

        if trend[i - 1] == 1:
            if low[i] > psar[i]:
                trend[i] = 1
                if high[i] > ep[i - 1]:
                    ep[i] = high[i]
                    af_values[i] = min(af_values[i - 1] + af, max_af)
                else:
                    ep[i] = ep[i - 1]
                    af_values[i] = af_values[i - 1]
            else:
                trend[i] = -1
                ep[i] = low[i]
                af_values[i] = af
        else:
            if high[i] < psar[i]:
                trend[i] = -1
                if low[i] < ep[i - 1]:
                    ep[i] = low[i]
                    af_values[i] = min(af_values[i - 1] + af, max_af)
                else:
                    ep[i] = ep[i - 1]
                    af_values[i] = af_values[i - 1]
            else:
                trend[i] = 1
                ep[i] = high[i]
                af_values[i] = af

    # Calculate sentiment score
    if trend[-1] == 1:
        sentiment = int(50 + (current_price - psar[-1]) / (high[-1] - low[-1]) * 50)
    else:
        sentiment = int(50 - (psar[-1] - current_price) / (high[-1] - low[-1]) * 50)
    sentiment = max(1, min(100, sentiment))

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def stochastic_momentum_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Stochastic Momentum Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    def calculate_stochastic_momentum(data):
        # Calculate Stochastic Oscillator
        k_period = 14
        d_period = 3

        low_min = historical_data["low"].rolling(window=k_period).min()
        high_max = historical_data["high"].rolling(window=k_period).max()

        k = 100 * (historical_data["close"] - low_min) / (high_max - low_min)
        d = k.rolling(window=d_period).mean()

        # Calculate Momentum
        momentum = historical_data["close"].diff(k_period)

        # Combine signals
        signal = (
            (k.iloc[-1] - 50)
            + (d.iloc[-1] - 50)
            + (momentum.iloc[-1] / historical_data["close"].iloc[-1] * 100)
        )

        # Normalize to 0-100 range
        normalized_signal = (signal + 200) / 4  # Assuming max signal is ±200
        return max(0, min(100, normalized_signal))

    sentiment = calculate_stochastic_momentum(historical_data)

    if sentiment > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment > 60:
        action = "buy"
        quantity = min(
            int((max_investment // 2) // current_price),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def williams_vix_fix_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Williams VIX Fix Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 22

    def calculate_williams_vix_fix(data):
        # Calculate highest high and lowest low
        highest_high = historical_data["high"].rolling(window=window).max()
        # lowest_low = historical_data["low"].rolling(window=window).min()

        # Calculate Williams VIX Fix
        wvf = ((highest_high - historical_data["low"]) / highest_high) * 100

        # Calculate Bollinger Bands for WVF
        wvf_sma = wvf.rolling(window=window).mean()
        wvf_std = wvf.rolling(window=window).std()
        upper_band = wvf_sma + (2 * wvf_std)

        # Generate signal
        if wvf.iloc[-1] > upper_band.iloc[-1]:
            return 90  # Strong buy signal
        elif wvf.iloc[-1] > wvf_sma.iloc[-1]:
            return 70  # Buy signal
        elif wvf.iloc[-1] < wvf_sma.iloc[-1]:
            return 30  # Sell signal
        else:
            return 50  # Hold signal

    sentiment = calculate_williams_vix_fix(historical_data)

    if sentiment > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment > 60:
        action = "buy"
        quantity = min(
            int((max_investment // 2) // current_price),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def conners_rsi_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    rsi_period = 3
    # streak_period = 2
    rank_period = 100

    # Calculate RSI
    delta = historical_data["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.dropna()  # Handle NaNs

    # Calculate Streak RSI
    streak = np.zeros(len(historical_data))
    for i in range(1, len(historical_data)):
        if historical_data["close"].iloc[i] > historical_data["close"].iloc[i - 1]:
            streak[i] = streak[i - 1] + 1
        elif historical_data["close"].iloc[i] < historical_data["close"].iloc[i - 1]:
            streak[i] = streak[i - 1] - 1

    # Convert streak to a pandas Series and drop NaNs
    streak_series = pd.Series(streak)
    streak_rsi = (
        100
        * (streak_series - streak_series.min())
        / (streak_series.max() - streak_series.min())
    ).dropna()

    # Calculate Percentile Rank
    def rolling_percentile_rank(series, window):
        return (
            series.rolling(window)
            .apply(lambda x: stats.percentileofscore(x, x.iloc[-1]), raw=False)
            .fillna(0)
        )

    rank = rolling_percentile_rank(historical_data["close"], rank_period).dropna()

    # Reset index for each series
    rsi = rsi.reset_index(drop=True)
    streak_rsi = streak_rsi.reset_index(drop=True)
    rank = rank.reset_index(drop=True)
    """
    print("RSI length:", len(rsi))
    print("Streak RSI length:", len(streak_rsi))
    print("Rank length:", len(rank))
    print("RSI NaN count:", rsi.isnull().sum())
    print("Streak RSI NaN count:", streak_rsi.isnull().sum())
    print("Rank NaN count:", rank.isnull().sum())
    """

    # Combine all components to form CRSI
    crsi = (rsi + streak_rsi + rank) / 3
    crsi = crsi.dropna()  # Ensure final CRSI has no NaN values

    # Calculate sentiment score (1-100)
    if not crsi.empty:
        sentiment = 100 - crsi.iloc[-1]
    else:
        return "hold", 0, ticker  # Return hold if CRSI couldn't be calculated

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def dpo_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Detrended Price Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 20

    # Calculate DPO
    shift = period // 2 + 1
    ma = historical_data["close"].rolling(window=period).mean()
    dpo = historical_data["close"].shift(shift) - ma

    # Calculate sentiment score (1-100)
    max_dpo = dpo.abs().max()
    sentiment = 50 + (dpo.iloc[-1] / max_dpo) * 50

    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def fisher_transform_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Fisher Transform Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def compute_fisher_transform(data):
        # Calculate the median price
        median_price = (historical_data["high"] + historical_data["low"]) / 2

        # Normalize price
        normalized = (median_price - median_price.rolling(window).min()) / (
            median_price.rolling(window).max() - median_price.rolling(window).min()
        )

        # Calculate Fisher Transform
        fisher = 0.5 * np.log((1 + normalized) / (1 - normalized))
        signal = fisher.shift(1)

        return fisher, signal

    fisher, signal = compute_fisher_transform(historical_data)

    # Calculate sentiment score
    if fisher.iloc[-1] > signal.iloc[-1] and fisher.iloc[-1] < 0:
        sentiment = (
            80 + (fisher.iloc[-1] - signal.iloc[-1]) * 100
        )  # Scale to 61-100 range
    elif fisher.iloc[-1] < signal.iloc[-1] and fisher.iloc[-1] > 0:
        sentiment = (
            20 - (signal.iloc[-1] - fisher.iloc[-1]) * 100
        )  # Scale to 1-40 range
    else:
        fisher = fisher.replace([np.inf, -np.inf], np.nan)
        signal = signal.replace([np.inf, -np.inf], np.nan)
        sentiment = (
            50 + (fisher.iloc[-1] - signal.iloc[-1]) * 50
        )  # Scale to 41-60 range

    sentiment = max(1, min(100, sentiment))  # Ensure sentiment is between 1 and 100

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int((max_investment // 2) // current_price),
            int(account_cash // current_price),
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def ehlers_fisher_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Ehlers Fisher Transform Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    def calculate_fisher(data):
        period = 10
        ema = data["close"].ewm(span=period, adjust=False).mean()

        normalized = (ema - ema.rolling(period).min()) / (
            ema.rolling(period).max() - ema.rolling(period).min()
        )
        normalized = 0.999 * normalized  # Ensure bounds

        fisher = 0.5 * np.log((1 + normalized) / (1 - normalized))
        trigger = fisher.shift(1)

        return fisher, trigger

    fisher, trigger = calculate_fisher(historical_data)

    # Calculate sentiment score (1-100)
    sentiment = 50 + (fisher.iloc[-1] - trigger.iloc[-1]) * 25
    sentiment = max(1, min(100, sentiment))

    if sentiment > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment > 60:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def schaff_trend_cycle_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Schaff Trend Cycle Strategy
    """
    max_investment = total_portfolio_value * 0.10
    stc_period = 10
    fast_period = 23
    slow_period = 50

    def calculate_stc(data):
        exp1 = data["close"].ewm(span=fast_period, adjust=False).mean()
        exp2 = data["close"].ewm(span=slow_period, adjust=False).mean()
        macd = exp1 - exp2

        def stc_calculation(x):
            lowest_low = x.rolling(window=stc_period).min()
            highest_high = x.rolling(window=stc_period).max()
            k = 100 * (x - lowest_low) / (highest_high - lowest_low)
            return k

        k = stc_calculation(macd)
        d = stc_calculation(k)

        return d

    stc = calculate_stc(historical_data)

    # Calculate sentiment score (1-100)
    sentiment = stc.iloc[-1]

    if sentiment > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment > 60:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def rainbow_oscillator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Rainbow Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    periods = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

    # Calculate multiple SMAs
    smas = pd.DataFrame()
    for period in periods:
        smas[f"SMA_{period}"] = historical_data["close"].rolling(window=period).mean()

    # Calculate Rainbow Oscillator
    highest = smas.max(axis=1)
    lowest = smas.min(axis=1)
    rainbow = ((historical_data["close"] - lowest) / (highest - lowest)) * 100

    # Calculate sentiment score
    sentiment = rainbow.iloc[-1]

    # Determine action and quantity based on sentiment
    if sentiment < 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif sentiment < 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.25)))
    elif 60 <= sentiment < 80:
        action = "buy"
        quantity = min(
            int(max_investment * 0.25 // current_price),
            int(account_cash // current_price),
        )
    elif sentiment >= 80:
        action = "strong buy"
        quantity = min(
            int(max_investment * 0.5 // current_price),
            int(account_cash // current_price),
        )
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def heikin_ashi_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Heikin Ashi Candlestick Strategy
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    # Calculate Heikin Ashi candles
    ha_close = (
        historical_data["open"]
        + historical_data["high"]
        + historical_data["low"]
        + historical_data["close"]
    ) / 4
    ha_open = pd.Series(index=historical_data.index)
    ha_open.iloc[0] = historical_data["open"].iloc[0]
    for i in range(1, len(historical_data)):
        ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2

    # ha_high = historical_data[["high", "open", "close"]].max(axis=1)
    # ha_low = historical_data[["low", "open", "close"]].min(axis=1)

    # Generate signals based on Heikin Ashi patterns
    bullish_candles = (ha_close > ha_open).rolling(window=3).sum()
    bearish_candles = (ha_close < ha_open).rolling(window=3).sum()

    # Calculate sentiment score
    sentiment = 50 + (bullish_candles.iloc[-1] - bearish_candles.iloc[-1]) * 10

    # Determine action and quantity based on sentiment
    if sentiment >= 80:
        action = "strong buy"
        quantity = min(
            int(max_investment * 0.5 // current_price),
            int(account_cash // current_price),
        )
    elif sentiment >= 60:
        action = "buy"
        quantity = min(
            int(max_investment * 0.25 // current_price),
            int(account_cash // current_price),
        )
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.25)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def volume_weighted_macd_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Volume-Weighted MACD Strategy: Combines MACD with volume for stronger signals
    """
    # Calculate volume-weighted price
    vwp = (historical_data["close"] * historical_data["volume"]).rolling(
        window=12
    ).sum() / historical_data["volume"].rolling(window=12).sum()

    # Calculate VWMACD
    exp1 = vwp.ewm(span=12, adjust=False).mean()
    exp2 = vwp.ewm(span=26, adjust=False).mean()
    vwmacd = exp1 - exp2
    signal = vwmacd.ewm(span=9, adjust=False).mean()

    # Sentiment Score Calculation
    if vwmacd.iloc[-1] > signal.iloc[-1] and vwmacd.iloc[-2] <= signal.iloc[-2]:
        sentiment_score = 80  # Buy
    elif vwmacd.iloc[-1] < signal.iloc[-1] and vwmacd.iloc[-2] >= signal.iloc[-2]:
        sentiment_score = 20  # Sell
    else:
        sentiment_score = 50  # Hold

    # Quantity Calculation
    max_investment = total_portfolio_value * 0.10
    if sentiment_score >= 80:
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment_score >= 60:
        quantity = min(
            int(max_investment * 0.5 // current_price),
            int(account_cash // current_price),
        )
    elif sentiment_score <= 20 and portfolio_qty > 0:
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment_score <= 40 and portfolio_qty > 0:
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.25)))
    else:
        quantity = 0

    # Sentiment String
    if sentiment_score >= 80:
        sentiment = "strong buy"
    elif sentiment_score >= 60:
        sentiment = "buy"
    elif sentiment_score <= 20 and portfolio_qty > 0:
        sentiment = "strong sell"
    elif sentiment_score <= 40 and portfolio_qty > 0:
        sentiment = "sell"
    else:
        sentiment = "hold"

    return sentiment, quantity, ticker


def fractal_adaptive_moving_average_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Fractal Adaptive Moving Average (FRAMA) Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def calculate_frama(data):
        # Calculate Fractal Dimension
        def calc_fractal_dimension(high, low, period):
            n1 = (
                (
                    np.log(high.rolling(period).max() - low.rolling(period).min())
                    - np.log(high - low)
                )
                .rolling(period)
                .mean()
            )
            n2 = np.log(period) * 0.5
            dimension = 2 - (n1 / n2)
            return dimension

        prices = data["close"]
        high = data["high"]
        low = data["low"]

        fd = calc_fractal_dimension(high, low, window)
        alpha = np.exp(-4.6 * (fd - 1))
        frama = prices.copy()

        for i in range(window, len(prices)):
            frama.iloc[i] = (
                alpha.iloc[i] * prices.iloc[i] + (1 - alpha.iloc[i]) * frama.iloc[i - 1]
            )

        return frama

    frama = calculate_frama(historical_data)

    # Calculate sentiment score
    if current_price > frama.iloc[-1]:
        sentiment = min(
            100, 60 + 40 * (current_price - frama.iloc[-1]) / frama.iloc[-1]
        )
    else:
        sentiment = max(1, 60 - 40 * (frama.iloc[-1] - current_price) / frama.iloc[-1])

    # Determine action and quantity based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment < 81:
        action = "buy"
        quantity = min(
            int((max_investment // 2) // current_price),
            int(account_cash // current_price),
        )
    elif 41 <= sentiment < 61:
        action = "hold"
        quantity = 0
    elif 21 <= sentiment < 41 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.25)))
    elif sentiment < 21 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def relative_vigor_index_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Relative Vigor Index (RVI) Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 10

    # Calculate RVI
    close_open = historical_data["close"] - historical_data["open"]
    high_low = historical_data["high"] - historical_data["low"]

    num = close_open.rolling(window).mean()
    den = high_low.rolling(window).mean()

    rvi = num / den
    signal = rvi.rolling(4).mean()

    # Calculate sentiment score
    if rvi.iloc[-1] > signal.iloc[-1] and rvi.iloc[-2] <= signal.iloc[-2]:
        sentiment = 90  # Strong buy
    elif rvi.iloc[-1] > signal.iloc[-1]:
        sentiment = 70  # Buy
    elif rvi.iloc[-1] < signal.iloc[-1] and rvi.iloc[-2] >= signal.iloc[-2]:
        sentiment = 10  # Strong sell
    elif rvi.iloc[-1] < signal.iloc[-1]:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 41 <= sentiment <= 60:
        action = "hold"
        quantity = 0
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return (action, quantity, ticker)


def center_of_gravity_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Center of Gravity Oscillator Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 10

    # Calculate Center of Gravity
    prices = historical_data["close"].values
    weights = np.arange(1, window + 1)
    cog = np.zeros_like(prices)

    for i in range(window - 1, len(prices)):
        window_prices = prices[i - window + 1 : i + 1]
        cog[i] = -np.sum(window_prices * weights) / np.sum(window_prices)

    cog_series = pd.Series(cog, index=historical_data.index)
    signal = cog_series.rolling(3).mean()

    # Calculate sentiment score
    if cog_series.iloc[-1] > signal.iloc[-1] and cog_series.iloc[-2] <= signal.iloc[-2]:
        sentiment = 90  # Strong buy
    elif cog_series.iloc[-1] > signal.iloc[-1]:
        sentiment = 70  # Buy
    elif (
        cog_series.iloc[-1] < signal.iloc[-1] and cog_series.iloc[-2] >= signal.iloc[-2]
    ):
        sentiment = 10  # Strong sell
    elif cog_series.iloc[-1] < signal.iloc[-1]:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def kauffman_efficiency_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Kaufman Efficiency Ratio Strategy with Adaptive Parameters
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def calculate_efficiency_ratio(data):
        # Calculate Price Change and Volatility
        price_change = abs(data["close"] - data["close"].shift(window))
        volatility = data["close"].diff().abs().rolling(window).sum()

        # Calculate Efficiency Ratio
        efficiency_ratio = price_change / volatility

        return efficiency_ratio.iloc[-1]

    efficiency_ratio = calculate_efficiency_ratio(historical_data)

    # Convert efficiency ratio to sentiment score (1-100)
    sentiment = min(100, max(1, int(efficiency_ratio * 100)))

    # Determine action based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def phase_change_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Market Phase Change Detection Strategy
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def calculate_phase_change(data):
        returns = data["close"].pct_change()

        # Compute Hilbert Transform
        hilbert = returns.diff()
        phase = np.arctan2(hilbert, returns)

        # Smooth the phase
        smooth_phase = phase.rolling(window=window).mean()
        phase_change = smooth_phase.diff()

        # Calculate momentum
        momentum = returns.rolling(window=window).mean()

        return phase_change.iloc[-1], momentum.iloc[-1]

    phase_change, momentum = calculate_phase_change(historical_data)

    # Convert phase change and momentum to sentiment score (1-100)
    sentiment = min(100, max(1, int((phase_change + momentum) * 50 + 50)))

    # Determine action based on sentiment
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def volatility_breakout_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Volatility-Based Breakout Strategy
    """
    max_investment = total_portfolio_value * 0.10
    period = 20

    # Calculate ATR-based volatility
    high_low = historical_data["high"] - historical_data["low"]
    high_close = abs(historical_data["high"] - historical_data["close"].shift())
    low_close = abs(historical_data["low"] - historical_data["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.rolling(window=period).mean()

    # Calculate volatility bands
    upper_band = historical_data["close"].rolling(period).mean() + (2 * atr)
    lower_band = historical_data["close"].rolling(period).mean() - (2 * atr)

    # Volume confirmation
    volume_ma = historical_data["volume"].rolling(period).mean()

    # Calculate sentiment score
    if (
        current_price > upper_band.iloc[-1]
        and historical_data["volume"].iloc[-1] > 1.5 * volume_ma.iloc[-1]
    ):
        sentiment = 90  # Strong buy
    elif current_price > upper_band.iloc[-1]:
        sentiment = 70  # Buy
    elif current_price < lower_band.iloc[-1] and portfolio_qty > 0:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def momentum_divergence_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Momentum Divergence Strategy with Multiple Timeframes
    """
    max_investment = total_portfolio_value * 0.10
    short_period = 14
    # long_period = 28

    # Calculate RSI for multiple timeframes
    def calculate_rsi(data, period):
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    short_rsi = calculate_rsi(historical_data["close"], short_period)
    # long_rsi = calculate_rsi(historical_data["close"], long_period)

    # Detect divergence
    price_trend = historical_data["close"].diff(short_period).iloc[-1]
    rsi_trend = short_rsi.diff(short_period).iloc[-1]

    # Calculate sentiment score
    if price_trend < 0 and rsi_trend > 0 and short_rsi.iloc[-1] < 30:
        sentiment = 90  # Strong buy
    elif price_trend > 0 and rsi_trend < 0 and short_rsi.iloc[-1] > 70:
        sentiment = 10  # Strong sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def adaptive_channel_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    base_period = 20

    volatility = historical_data["close"].pct_change().std()
    adaptive_period = int(base_period * (1 + volatility))

    upper_channel = historical_data["high"].rolling(adaptive_period).max()
    lower_channel = historical_data["low"].rolling(adaptive_period).min()
    middle_channel = (upper_channel + lower_channel) / 2

    channel_width = (upper_channel - lower_channel) / middle_channel

    if (
        current_price > upper_channel.iloc[-1]
        and channel_width.iloc[-1] > channel_width.rolling(base_period).mean().iloc[-1]
    ):
        # sentiment = 90  # Strong buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "strong buy", quantity, ticker
    elif current_price > middle_channel.iloc[-1]:
        # sentiment = 70  # Buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "buy", quantity, ticker
    elif current_price < lower_channel.iloc[-1] and portfolio_qty > 0:
        # sentiment = 10  # Strong sell
        quantity = portfolio_qty
        return "strong sell", quantity, ticker
    elif current_price < middle_channel.iloc[-1] and portfolio_qty > 0:
        # sentiment = 30  # Sell
        quantity = portfolio_qty
        return "sell", quantity, ticker
    else:
        # sentiment = 50  # Hold
        return "hold", 0, ticker


def wavelet_decomposition_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    window = 20

    def compute_wavelet_features(data):
        returns = data["close"].pct_change().fillna(0)

        # Calculate the maximum level based on data length
        max_level = pywt.dwt_max_level(len(returns), "db4")
        level = min(
            2, max_level
        )  # Use 2 or the maximum possible level, whichever is smaller

        # Wavelet decomposition using PyWavelets
        coeffs = pywt.wavedec(returns, "db4", level=level)

        # Energy at different scales
        energies = [np.sum(c**2) for c in coeffs]
        return np.mean(energies)

    wavelet_signal = pd.Series(index=historical_data.index)
    for i in range(window, len(historical_data)):
        wavelet_signal.iloc[i] = compute_wavelet_features(
            historical_data.iloc[i - window : i]
        )

    signal_strength = (
        wavelet_signal.iloc[-1] - wavelet_signal.mean()
    ) / wavelet_signal.std()

    if signal_strength > 1.5:
        # sentiment = 90  # Strong buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "strong buy", quantity, ticker
    elif signal_strength > 0.5:
        # sentiment = 70  # Buy
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        return "buy", quantity, ticker
    elif signal_strength < -1.5 and portfolio_qty > 0:
        # sentiment = 10  # Strong sell
        quantity = portfolio_qty
        return "strong sell", quantity, ticker
    elif signal_strength < -0.5 and portfolio_qty > 0:
        # sentiment = 30  # Sell
        quantity = portfolio_qty
        return "sell", quantity, ticker
    else:
        # sentiment = 50  # Hold
        return "hold", 0, ticker


def entropy_flow_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Statistical Entropy Flow Strategy
    Uses entropy dynamics and information flow
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    def compute_entropy_flow(data):
        returns = data["close"].pct_change().fillna(0)
        volumes = data["volume"].pct_change().fillna(0)

        # Compute joint entropy using scipy.stats
        hist, _ = np.histogramdd([returns, volumes], bins=10)
        prob = hist / np.sum(hist)
        entropy = scipy.stats.entropy(prob.flatten())

        # Compute flow using numpy gradient
        flow = np.gradient(entropy * np.abs(returns))
        return np.mean(flow)

    entropy_signal = pd.Series(index=historical_data.index)
    for i in range(window, len(historical_data)):
        entropy_signal.iloc[i] = compute_entropy_flow(
            historical_data.iloc[i - window : i]
        )

    # Calculate sentiment score
    sentiment = (
        50
        + (entropy_signal.iloc[-1] - entropy_signal.mean()) / entropy_signal.std() * 25
    )
    sentiment = max(1, min(100, sentiment))  # Clamp between 1 and 100

    if sentiment > 80:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment > 60:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif sentiment < 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif sentiment < 40 and portfolio_qty > 0:
        action = "sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def bollinger_band_width_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    window = 20
    num_std = 2

    historical_data["MA"] = historical_data["close"].rolling(window=window).mean()
    historical_data["STD"] = historical_data["close"].rolling(window=window).std()
    historical_data["Upper"] = historical_data["MA"] + (
        num_std * historical_data["STD"]
    )
    historical_data["Lower"] = historical_data["MA"] - (
        num_std * historical_data["STD"]
    )

    upper_band = historical_data["Upper"].iloc[-1]
    lower_band = historical_data["Lower"].iloc[-1]

    # Calculate sentiment score
    range_size = upper_band - lower_band
    position = current_price - lower_band
    sentiment_score = (position / range_size) * 100

    if sentiment_score <= 20 and portfolio_qty > 0:
        quantity_to_sell = portfolio_qty  # Strong sell
        return ("strong sell", quantity_to_sell, ticker)
    elif 20 < sentiment_score <= 40 and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))  # Sell
        return ("sell", quantity_to_sell, ticker)
    elif 60 < sentiment_score <= 80 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )  # Buy
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)
    elif sentiment_score > 80 and account_cash > 0:
        quantity_to_buy = min(
            int((max_investment * 1.5) // current_price),
            int(account_cash // current_price),
        )  # Strong buy
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Hold (neutral sentiment or no portfolio quantity for sell signals)
    return ("hold", 0, ticker)


def commodity_channel_index_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate CCI
    tp = (
        historical_data["high"] + historical_data["low"] + historical_data["close"]
    ) / 3
    cci = (tp - tp.rolling(window=window).mean()) / (
        0.015 * tp.rolling(window=window).std()
    )

    current_cci = cci.iloc[-1]

    if current_cci < -200 and portfolio_qty > 0:
        quantity_to_sell = portfolio_qty  # Strong sell
        return ("strong sell", quantity_to_sell, ticker)
    elif -200 <= current_cci < -100 and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))  # Sell
        return ("sell", quantity_to_sell, ticker)
    elif 100 < current_cci <= 200 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )  # Buy
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)
    elif current_cci > 200 and account_cash > 0:
        quantity_to_buy = min(
            int((max_investment * 1.5) // current_price),
            int(account_cash // current_price),
        )  # Strong buy
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Hold (neutral sentiment or no portfolio quantity for sell signals)
    return ("hold", 0, ticker)


def force_index_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    window = 13

    # Calculate Force Index
    fi = historical_data["close"].diff() * historical_data["volume"]
    fi_ema = fi.ewm(span=window, adjust=False).mean()

    current_fi = fi_ema.iloc[-1]

    if current_fi < 0 and portfolio_qty > 0:
        quantity_to_sell = portfolio_qty  # Strong sell
        return ("strong sell", quantity_to_sell, ticker)
    elif 0 <= current_fi < 100 and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))  # Sell
        return ("sell", quantity_to_sell, ticker)
    elif 100 < current_fi <= 200 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )  # Buy
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)
    elif current_fi > 200 and account_cash > 0:
        quantity_to_buy = min(
            int((max_investment * 1.5) // current_price),
            int(account_cash // current_price),
        )  # Strong buy
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Hold (neutral sentiment or insufficient portfolio quantity)
    return ("hold", 0, ticker)


def ichimoku_cloud_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    max_investment = total_portfolio_value * 0.10
    window1 = 9
    window2 = 26
    window3 = 52

    # Calculate Ichimoku Cloud
    conv_line = (
        historical_data["high"].rolling(window=window1).max()
        + historical_data["low"].rolling(window=window1).min()
    ) / 2
    base_line = (
        historical_data["high"].rolling(window=window2).max()
        + historical_data["low"].rolling(window=window2).min()
    ) / 2
    leading_span_a = (conv_line + base_line) / 2
    leading_span_b = (
        historical_data["high"].rolling(window=window3).max()
        + historical_data["low"].rolling(window=window3).min()
    ) / 2

    current_conv_line = conv_line.iloc[-1]
    current_base_line = base_line.iloc[-1]
    current_leading_span_a = leading_span_a.iloc[-1]
    current_leading_span_b = leading_span_b.iloc[-1]

    if current_price < current_leading_span_b and portfolio_qty > 0:
        quantity_to_sell = portfolio_qty  # Strong sell
        return ("strong sell", quantity_to_sell, ticker)
    elif current_price < current_base_line and portfolio_qty > 0:
        quantity_to_sell = max(1, int(portfolio_qty * 0.5))  # Sell
        return ("sell", quantity_to_sell, ticker)
    elif current_price > current_conv_line and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )  # Buy
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)
    elif current_price > current_leading_span_a and account_cash > 0:
        quantity_to_buy = min(
            int((max_investment * 1.5) // current_price),
            int(account_cash // current_price),
        )  # Strong buy
        if quantity_to_buy > 0:
            return ("strong buy", quantity_to_buy, ticker)

    # Hold (neutral sentiment or insufficient portfolio quantity)
    return ("hold", 0, ticker)


def klinger_oscillator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """Klinger Oscillator Strategy"""
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Klinger Oscillator
    kvo = historical_data["volume"] * (
        historical_data["close"] - historical_data["close"].shift(1)
    )
    kvo = kvo.rolling(window=window).mean()

    # Calculate sentiment score
    sentiment = 50 + (kvo.iloc[-1] - kvo.mean()) / kvo.std() * 25
    sentiment = max(1, min(100, sentiment))  # Clamp between 1 and 100

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def money_flow_index_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """Money Flow Index Strategy"""
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Money Flow Index
    mfi = historical_data["volume"] * (
        historical_data["close"] - historical_data["close"].shift(1)
    )
    mfi = mfi.rolling(window=window).mean()

    # Calculate sentiment score
    sentiment = 50 + (mfi.iloc[-1] - mfi.mean()) / mfi.std() * 25
    sentiment = max(1, min(100, sentiment))  # Clamp between 1 and 100

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def on_balance_volume_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """On Balance Volume Strategy"""
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate On Balance Volume
    obv = historical_data["volume"] * np.sign(
        historical_data["close"] - historical_data["close"].shift(1)
    )
    obv = obv.rolling(window=window).mean()

    # Calculate sentiment score
    sentiment = 50 + (obv.iloc[-1] - obv.mean()) / obv.std() * 25
    sentiment = max(1, min(100, sentiment))  # Clamp between 1 and 100

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def stochastic_oscillator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """Stochastic Oscillator Strategy"""
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Stochastic Oscillator
    low_min = historical_data["low"].rolling(window=window).min()
    high_max = historical_data["high"].rolling(window=window).max()
    k = 100 * (historical_data["close"] - low_min) / (high_max - low_min)
    d = k.rolling(window=3).mean()

    # Calculate sentiment score
    sentiment = 50 + (k.iloc[-1] - d.iloc[-1]) * 25
    sentiment = max(1, min(100, sentiment))  # Clamp between 1 and 100

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def euler_fibonacci_zone_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Euler-Fibonacci Zone Trading Strategy
    This strategy combines Euler's number (e) with Fibonacci ratios to create trading zones.
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Euler-Fibonacci zones
    high = historical_data["high"].rolling(window=window).max()
    low = historical_data["low"].rolling(window=window).min()
    range_size = high - low

    euler = np.exp(1)  # Euler's number
    fib_ratios = [0.236, 0.382, 0.618, 1.0, 1.618]

    zones = []
    for ratio in fib_ratios:
        zone = low + (range_size * ratio * euler) % range_size
        zones.append(zone)

    # current_zone = zones[-1].iloc[-1]  # Use the last calculated zone

    # Calculate sentiment score
    if current_price < zones[0].iloc[-1]:
        sentiment = 90  # Strong buy
    elif current_price < zones[1].iloc[-1]:
        sentiment = 70  # Buy
    elif current_price > zones[4].iloc[-1]:
        sentiment = 10  # Strong sell
    elif current_price > zones[3].iloc[-1]:
        sentiment = 30  # Sell
    else:
        sentiment = 50  # Hold

    # Determine action and quantity
    if sentiment >= 81:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 61 <= sentiment <= 80:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker
