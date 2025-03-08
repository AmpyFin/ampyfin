import numpy as np
import pywt  # PyWavelets for wavelet analysis
import ripser  # For topological data analysis
from scipy.special import zeta  # For Riemann zeta function

# 12/7/2024 talib is now available so please use those functions. This file is no longer in use


def pairs_trading_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Pairs Trading Strategy using correlation and z-score
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate z-score of price ratio
    returns = historical_data["close"].pct_change()
    z_score = (returns - returns.rolling(window).mean()) / returns.rolling(window).std()

    # Generate signals based on z-score
    if z_score.iloc[-1] < -2:
        sentiment = 90  # Strong buy
    elif z_score.iloc[-1] < -1:
        sentiment = 70  # Buy
    elif z_score.iloc[-1] > 2:
        sentiment = 10  # Strong sell
    elif z_score.iloc[-1] > 1:
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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def kalman_filter_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Kalman Filter Strategy for dynamic trend following
    """
    max_investment = total_portfolio_value * 0.10

    # Initialize Kalman Filter parameters
    Q = 0.01  # Process variance
    R = 1.0  # Measurement variance
    P = 1.0  # Initial estimation error covariance
    K = 0.0  # Initial Kalman Gain
    x = historical_data["close"].iloc[0]  # Initial estimate

    estimates = []
    for price in historical_data["close"]:
        # Prediction
        P = P + Q

        # Update
        K = P / (P + R)
        x = x + K * (price - x)
        P = (1 - K) * P
        estimates.append(x)

    kalman_estimate = estimates[-1]

    # Calculate sentiment based on Kalman estimate
    diff_percent = (current_price - kalman_estimate) / kalman_estimate * 100
    sentiment = 50 + diff_percent
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def regime_switching_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Regime Switching Strategy using volatility states
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate volatility regimes
    returns = historical_data["close"].pct_change()
    volatility = returns.rolling(window=window).std()
    high_vol_threshold = volatility.mean() + volatility.std()
    low_vol_threshold = volatility.mean() - volatility.std()

    current_vol = volatility.iloc[-1]

    # Determine regime and calculate momentum
    momentum = returns.rolling(window=window).mean().iloc[-1]

    if current_vol < low_vol_threshold:
        # Low volatility regime - more aggressive
        sentiment = 50 + (momentum * 200)
    elif current_vol > high_vol_threshold:
        # High volatility regime - more conservative
        sentiment = 50 + (momentum * 100)
    else:
        # Normal regime
        sentiment = 50 + (momentum * 150)

    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def adaptive_momentum_filter_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Adaptive Momentum Filter Strategy using dynamic timeframes
    """
    max_investment = total_portfolio_value * 0.10

    # Calculate adaptive lookback period based on market volatility
    volatility = historical_data["close"].pct_change().std()
    adaptive_window = int(max(10, min(50, 20 / volatility)))

    # Calculate momentum indicators with adaptive window
    roc = historical_data["close"].pct_change(periods=adaptive_window)
    ma = historical_data["close"].rolling(window=adaptive_window).mean()

    # Calculate trend strength
    trend_strength = abs(ma.pct_change(adaptive_window))

    # Combine signals for sentiment
    momentum_signal = roc.iloc[-1]
    trend_signal = (current_price - ma.iloc[-1]) / ma.iloc[-1]

    # Weight signals based on trend strength
    sentiment = 50 + (
        momentum_signal * 100 * trend_strength.iloc[-1]
        + trend_signal * 100 * (1 - trend_strength.iloc[-1])
    )

    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def fractal_market_hypothesis_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Fractal Market Hypothesis Strategy using Hurst Exponent
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate Hurst Exponent
    def hurst_exponent(prices, lags):
        tau = []
        lagvec = []
        for lag in lags:
            tau.append(np.sqrt(np.std(np.subtract(prices[lag:], prices[:-lag]))))
            lagvec.append(lag)
        return np.polyfit(np.log(lagvec), np.log(tau), 1)[0]

    # Calculate rolling Hurst exponent
    prices = historical_data["close"].values
    lags = range(2, 20)
    hurst = hurst_exponent(prices[-window:], lags)

    # Generate trading signals based on Hurst exponent
    if hurst > 0.6:  # Trending market
        momentum = (prices[-1] - prices[-window]) / prices[-window]
        sentiment = 50 + (momentum * 100)
    elif hurst < 0.4:  # Mean-reverting market
        z_score = (prices[-1] - np.mean(prices[-window:])) / np.std(prices[-window:])
        sentiment = 50 - (z_score * 20)
    else:  # Random walk
        sentiment = 50

    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def topological_data_analysis_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Topological Data Analysis Strategy using Persistence Diagrams
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Prepare time series data for TDA
    prices = historical_data["close"].values[-window:]
    volumes = historical_data["volume"].values[-window:]
    point_cloud = np.column_stack((prices, volumes))

    # Calculate persistence diagram using Ripser
    diagrams = ripser.ripser(point_cloud)["dgms"]

    # Extract topological features
    if len(diagrams) > 0:
        persistence_lengths = diagrams[0][:, 1] - diagrams[0][:, 0]
        topological_signal = np.sum(persistence_lengths)

        # Generate sentiment based on topological features
        sentiment = 50 + (topological_signal * 10)
    else:
        sentiment = 50

    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def levy_distribution_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Levy Distribution Strategy for fat-tailed price movements
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate returns and fit Levy distribution
    returns = historical_data["close"].pct_change().dropna()
    levy_alpha = np.minimum(2, np.sqrt(1 / np.mean(returns[-window:] ** 2)))

    # Calculate tail probabilities
    current_return = returns.iloc[-1]
    tail_prob = 1 / (1 + abs(current_return) ** levy_alpha)

    # Generate sentiment based on tail probabilities and Levy alpha
    if levy_alpha < 1.5:  # Heavy tails
        sentiment = 50 + (1 - tail_prob) * 100 * np.sign(current_return)
    else:  # More normal distribution
        sentiment = 50 + current_return * 50

    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def information_flow_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Information Flow Strategy using Transfer Entropy
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate price and volume information flows
    price_changes = historical_data["close"].pct_change()
    volume_changes = historical_data["volume"].pct_change()

    # Compute transfer entropy using mutual information
    def mutual_information(x, y, bins=10):
        hist_xy = np.histogram2d(x, y, bins)[0]
        prob_xy = hist_xy / float(np.sum(hist_xy))
        prob_x = np.sum(prob_xy, axis=1)
        prob_y = np.sum(prob_xy, axis=0)
        return np.sum(prob_xy * np.log2(prob_xy / (prob_x[:, None] * prob_y[None, :])))

    # Calculate information flow metrics
    info_flow = mutual_information(
        price_changes[-window:].fillna(0), volume_changes[-window:].fillna(0)
    )

    # Generate sentiment based on information flow
    sentiment = 50 + (info_flow * 25 * np.sign(price_changes.iloc[-1]))
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def wavelet_momentum_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Wavelet Momentum Strategy using multi-scale decomposition
    """
    max_investment = total_portfolio_value * 0.10

    # Perform wavelet decomposition
    prices = historical_data["close"].values
    coeffs = pywt.wavedec(prices, "db4", level=3)

    # Analyze momentum at different scales
    short_term = coeffs[1][-5:].mean()
    medium_term = coeffs[2][-5:].mean()
    long_term = coeffs[3][-5:].mean()

    # Combine multi-scale momentum signals
    momentum_signal = 0.5 * short_term + 0.3 * medium_term + 0.2 * long_term
    sentiment = 50 + (momentum_signal * 100)
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def complex_network_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Complex Network Strategy using price correlation networks
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Create correlation network
    returns = historical_data["close"].pct_change().dropna()
    rolling_corr = returns.rolling(window).corr(returns.shift())

    # Calculate network metrics
    degree_centrality = np.mean(np.abs(rolling_corr.fillna(0)))
    clustering_coeff = np.mean(np.power(rolling_corr.fillna(0), 3))

    # Generate trading signal
    network_signal = (degree_centrality + clustering_coeff) / 2
    sentiment = 50 + (network_signal * 100)
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def zeta_potential_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Zeta Potential Strategy using Riemann zeta function
    """
    max_investment = total_portfolio_value * 0.10
    # window = 20

    # Calculate price changes and normalize
    returns = historical_data["close"].pct_change().dropna()
    norm_returns = (returns - returns.mean()) / returns.std()

    # Calculate zeta potential
    s = 2 + 1j * norm_returns.iloc[-1]
    zeta_value = abs(zeta(s))

    # Generate trading signal
    zeta_signal = (zeta_value - 1) / (zeta_value + 1)
    sentiment = 50 + (zeta_signal * 50)
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def quantum_oscillator_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Quantum Oscillator Strategy using quantum-inspired algorithms
    """
    max_investment = total_portfolio_value * 0.10
    window = 20

    # Calculate quantum oscillator components
    momentum = historical_data["close"].pct_change()
    phase = np.angle(np.fft.fft(momentum[-window:]))
    amplitude = np.abs(np.fft.fft(momentum[-window:]))

    # Combine quantum components
    quantum_signal = np.mean(amplitude * np.exp(1j * phase))

    # Generate trading signal
    sentiment = 50 + (np.real(quantum_signal) * 100)
    sentiment = max(1, min(100, sentiment))

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
    elif sentiment <= 20 and portfolio_qty > 0:
        action = "strong sell"
        quantity = portfolio_qty
    elif 21 <= sentiment <= 40 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker


def simple_trend_reversal_strategy(
    ticker,
    current_price,
    historical_data,
    account_cash,
    portfolio_qty,
    total_portfolio_value,
):
    """
    Simple Trend Reversal Strategy based on consecutive price movements
    """
    max_investment = total_portfolio_value * 0.10

    # Get last 3 days of price changes
    price_changes = historical_data["close"].diff()
    today_change = price_changes.iloc[-1]
    yesterday_change = price_changes.iloc[-2]
    day_before_change = price_changes.iloc[-3]

    # Strong buy: 3 consecutive down days
    if today_change < 0 and yesterday_change < 0 and day_before_change < 0:
        action = "strong buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )

    # Buy: 2 consecutive down days
    elif today_change < 0 and yesterday_change < 0:
        action = "buy"
        quantity = min(
            int(max_investment // current_price), int(account_cash // current_price)
        )

    # Strong sell: 3 consecutive up days
    elif (
        today_change > 0
        and yesterday_change > 0
        and day_before_change > 0
        and portfolio_qty > 0
    ):
        action = "strong sell"
        quantity = portfolio_qty

    # Sell: 2 consecutive up days
    elif today_change > 0 and yesterday_change > 0 and portfolio_qty > 0:
        action = "sell"
        quantity = min(portfolio_qty, max(1, int(portfolio_qty * 0.5)))

    # Hold: all other conditions
    else:
        action = "hold"
        quantity = 0

    return action, quantity, ticker
