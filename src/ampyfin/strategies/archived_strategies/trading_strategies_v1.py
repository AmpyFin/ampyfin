from datetime import datetime, timedelta

from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# 12/7/2024 talib is now available so please use those functions. This file is no longer in use


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
    RSI strategy: Buy when RSI is oversold, sell when overbought.
    """
    window = 14
    max_investment = total_portfolio_value * 0.10

    # Calculate RSI
    delta = historical_data["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    current_rsi = rsi.iloc[-1]

    # Buy signal: RSI below 30 (oversold)
    if current_rsi < 30 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Sell signal: RSI above 70 (overbought)
    elif current_rsi > 70 and portfolio_qty > 0:
        quantity_to_sell = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        return ("sell", quantity_to_sell, ticker)

    # No action triggered
    return ("hold", portfolio_qty, ticker)


def bollinger_bands_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Bollinger Bands strategy: Buy when price touches lower band, sell when it touches upper band.
    """
    window = 20
    num_std = 2
    max_investment = total_portfolio_value * 0.10

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

    # Buy signal: Price at or below lower band
    if current_price <= lower_band and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Sell signal: Price at or above upper band
    elif current_price >= upper_band and portfolio_qty > 0:
        quantity_to_sell = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        return ("sell", quantity_to_sell, ticker)

    # No action triggered
    return ("hold", portfolio_qty, ticker)


def macd_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    MACD strategy: Buy when MACD line crosses above signal line, sell when it crosses below.
    """
    max_investment = total_portfolio_value * 0.10

    # Calculate MACD
    exp1 = historical_data["close"].ewm(span=12, adjust=False).mean()
    exp2 = historical_data["close"].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()

    # Get the last two MACD and signal values
    macd_current, macd_prev = macd.iloc[-1], macd.iloc[-2]
    signal_current, signal_prev = signal.iloc[-1], signal.iloc[-2]

    # Buy signal: MACD line crosses above signal line
    if macd_prev <= signal_prev and macd_current > signal_current and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Sell signal: MACD line crosses below signal line
    elif (
        macd_prev >= signal_prev and macd_current < signal_current and portfolio_qty > 0
    ):
        quantity_to_sell = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        return ("sell", quantity_to_sell, ticker)

    # No action triggered
    return ("hold", portfolio_qty, ticker)


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

    # Buy signal (short MA crosses above long MA)
    if short_ma > long_ma and account_cash > 0:
        # Calculate amount to invest based on available cash and max investment
        amount_to_invest = min(account_cash, max_investment)
        quantity_to_buy = int(
            amount_to_invest // current_price
        )  # Calculate quantity to buy

        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Sell signal (short MA crosses below long MA)
    elif short_ma < long_ma and portfolio_qty > 0:
        # Sell 50% of the current holding, at least 1 share
        quantity_to_sell = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
        return ("sell", quantity_to_sell, ticker)

    # No action triggered
    return ("hold", portfolio_qty, ticker)


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

    # Calculate moving average
    window = 20  # Use a 20-day moving average
    historical_data["MA20"] = historical_data["close"].rolling(window=window).mean()

    # Drop NaN values after creating the moving average
    historical_data.dropna(inplace=True)

    # Define max investment (10% of total portfolio value)
    max_investment = total_portfolio_value * 0.10

    # Buy signal: if current price is below the moving average by more than 2%
    if current_price < historical_data["MA20"].iloc[-1] * 0.98 and account_cash > 0:
        quantity_to_buy = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )
        if quantity_to_buy > 0:
            return ("buy", quantity_to_buy, ticker)

    # Sell signal: if current price is above the moving average by more than 2%
    elif current_price > historical_data["MA20"].iloc[-1] * 1.02 and portfolio_qty > 0:
        quantity_to_sell = min(
            portfolio_qty, max(1, int(portfolio_qty * 0.5))
        )  # Sell 50% of portfolio, or at least 1
        return ("sell", quantity_to_sell, ticker)

    # No action triggered
    return ("hold", portfolio_qty, ticker)
