import numpy as np
import pandas as pd
import talib as ta

def _generate_signals(condition_buy, condition_sell, default=0):
    conditions = [condition_buy, condition_sell]
    choices = [1, -1]
    return np.select(conditions, choices, default=default)

def SMA_MOMENTUM_CROSSOVER_indicator(data, short_period=10, long_period=50):
    \"\"\"Vectorized SMA Momentum Crossover strategy.\"\"\"
    short_sma = ta.SMA(data["Close"], timeperiod=short_period)
    long_sma = ta.SMA(data["Close"], timeperiod=long_period)
    data["SMA_MOMENTUM_CROSSOVER_indicator"] = _generate_signals(
        condition_buy=(short_sma > long_sma) & (short_sma.shift(1) <= long_sma.shift(1)),
        condition_sell=(short_sma < long_sma) & (short_sma.shift(1) >= long_sma.shift(1))
    )
    return data["SMA_MOMENTUM_CROSSOVER_indicator"]
