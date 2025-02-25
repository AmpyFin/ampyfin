

from control import * 
from helper_files.train_client_helper import *
from helper_files.client_helper import * 
from strategies.talib_indicators import *

def initialize_simulation(period_start, period_end, train_tickers, mongo_client, FINANCIAL_PREP_API_KEY, logger):
    """
    Initializes the simulation by loading necessary data and setting up initial states.
    """
    logger.info("Initializing simulation...")

    ticker_price_history = {}
    ideal_period = {}

    # Connect to MongoDB and retrieve indicator periods
    db = mongo_client.IndicatorsDatabase
    indicator_collection = db.Indicators
    logger.info("Connected to MongoDB: Retrieved Indicators collection.")
    for strategy in strategies:
        
        period = indicator_collection.find_one({'indicator': strategy.__name__})
        if period:
            ideal_period[strategy.__name__] = period['ideal_period']
            logger.info(f"Retrieved ideal period for {strategy.__name__}: {period['ideal_period']}")
        else:
            logger.info(f"No ideal period found for {strategy.__name__}, using default.")

    # Fetch tickers if none are provided
    if not train_tickers:
        logger.info("No tickers provided. Fetching Nasdaq tickers...")
        train_tickers = get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)
        logger.info(f"Fetched {len(train_tickers)} tickers.")

    # Determine historical data start date
    start_date = datetime.strptime(period_start, "%Y-%m-%d")
    data_start_date = (start_date - timedelta(days=730)).strftime("%Y-%m-%d")
    logger.info(f"Fetching historical data from {data_start_date} to {period_end}.")

    for ticker in train_tickers:
        try:
            data = yf.Ticker(ticker).history(start=data_start_date, end=period_end, interval="1d")
            logger.info(f"Successfully retrieved data for {ticker}: {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}")
            ticker_price_history[ticker] = data
        except Exception as e:
            logger.info(f"Error retrieving specific date range for {ticker}, fetching max available data. Error: {str(e)}")
            data = yf.Ticker(ticker).history(period="max", interval="1d")
            logger.info(f"Successfully retrieved max available data for {ticker}: {data.iloc[0].name.date()} to {data.iloc[-1].name.date()}")
            ticker_price_history[ticker] = data

    logger.info("Simulation initialization complete.")
    return ticker_price_history, ideal_period


def update_time_delta(time_delta, mode):
    """
    Updates time_delta based on the specified mode
    """
    if mode == 'additive':
        return time_delta + train_time_delta_increment
    elif mode == 'multiplicative':
        return time_delta * train_time_delta_multiplicative
    elif mode == 'balanced':
        return time_delta + train_time_delta_balanced * time_delta
    return time_delta

def update_points_and_trades(strategy, ratio, current_price, trading_simulator, points, time_delta, ticker, qty):
    """
    Updates points based on trade performance and manages trade statistics
    """
    if current_price > trading_simulator[strategy.__name__]["holdings"][ticker]["price"]:
        trading_simulator[strategy.__name__]["successful_trades"] += 1
        if ratio < train_profit_price_change_ratio_d1:
            points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d1
        elif ratio < train_profit_price_change_ratio_d2:
            points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_d2
        else:
            points[strategy.__name__] = points.get(strategy.__name__, 0) + time_delta * train_profit_profit_time_else
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
        raise Exception("Quantity cannot be negative")
    trading_simulator[strategy.__name__]["total_trades"] += 1
    
    return points, trading_simulator

def execute_trade(decision, qty, ticker, current_price, strategy, trading_simulator, points, time_delta, portfolio_qty, total_portfolio_value):
    """
    Executes a trade based on the strategy decision and updates trading simulator and points
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
        
        points, trading_simulator = update_points_and_trades(
            strategy, ratio, current_price, trading_simulator, 
            points, time_delta, ticker, qty
        )
        
    return trading_simulator, points

def simulate_trading_day(current_date, strategies, trading_simulator, points, time_delta, ticker_price_history, train_tickers, ideal_period, logger):
    """
    Simulates trading activities for all strategies for a given day.
    """
    logger.info(f"Simulating trading for {current_date.strftime('%Y-%m-%d')}.")

    if not train_tickers:
        # logger.info("No tickers available for trading. Skipping simulation.")
        return trading_simulator, points

    for ticker in train_tickers:
        if current_date.strftime('%Y-%m-%d') in ticker_price_history[ticker].index:
            # logger.info(f"{ticker}: Data available for {current_date.strftime('%Y-%m-%d')}.")
            daily_data = ticker_price_history[ticker].loc[current_date.strftime('%Y-%m-%d')]
            current_price = daily_data['Close']
            # logger.info(f"{ticker}: Current price: {current_price}")

            for strategy in strategies:
                # logger.info(f"Processing strategy: {strategy.__name__} for {ticker}.")
                
                # Fetch historical data
                historical_data = get_historical_data(ticker, current_date, ideal_period[strategy.__name__], ticker_price_history)
                # logger.info(f"{ticker} - {strategy.__name__}: Retrieved historical data for strategy.")

                # Fetch account details
                account_cash = trading_simulator[strategy.__name__]["amount_cash"]
                portfolio_qty = trading_simulator[strategy.__name__]["holdings"].get(ticker, {}).get("quantity", 0)
                total_portfolio_value = trading_simulator[strategy.__name__]["portfolio_value"]

                # logger.info(f"{ticker} - {strategy.__name__}: Account Cash: {account_cash}, Portfolio Qty: {portfolio_qty}, Portfolio Value: {total_portfolio_value}")

                # Make trading decision
                decision, qty = simulate_strategy(
                    strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value
                )
                # logger.info(f"{ticker} - {strategy.__name__}: Decision: {decision}, Quantity: {qty}")

                # Execute trade
                trading_simulator, points = execute_trade(
                    decision, qty, ticker, current_price, strategy, trading_simulator, 
                    points, time_delta, portfolio_qty, total_portfolio_value
                )
                # logger.info(f"{ticker} - {strategy.__name__}: Trade executed.")

        else:
            logger.info(f"{ticker}: No data available for {current_date.strftime('%Y-%m-%d')}, skipping.")

    if not trading_simulator or not points:
        logger.info(f"Warning: Trading simulator or points dictionary is empty after simulation for {current_date.strftime('%Y-%m-%d')}.")

    logger.info(f"Completed simulation for {current_date.strftime('%Y-%m-%d')}.")
    return trading_simulator, points

