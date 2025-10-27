# Trading Strategies Guide

This document provides comprehensive information about the trading strategies implemented in AmpyFin, including TA-Lib indicators and custom strategies.

## Overview

AmpyFin implements over 100 trading strategies based on TA-Lib technical indicators, organized into categories. Each strategy generates buy/sell/hold signals that are weighted by the ranking system to make final trading decisions.

## Strategy Categories

### 1. Overlap Studies (19 indicators)

These indicators overlay on price charts and help identify trend direction and support/resistance levels.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `BBANDS_indicator` | Bollinger Bands | Buy when price touches lower band, sell when touching upper band |
| `DEMA_indicator` | Double Exponential Moving Average | Buy when price > DEMA, sell when price < DEMA |
| `EMA_indicator` | Exponential Moving Average | Buy when price > EMA, sell when price < EMA |
| `HT_TRENDLINE_indicator` | Hilbert Transform Trendline | Buy when price > trendline, sell when price < trendline |
| `KAMA_indicator` | Kaufman's Adaptive Moving Average | Buy when price > KAMA, sell when price < KAMA |
| `MA_indicator` | Simple Moving Average | Buy when price > MA, sell when price < MA |
| `MAMA_indicator` | MESA Adaptive Moving Average | Buy when price > MAMA, sell when price < MAMA |
| `MAVP_indicator` | Moving Average with Variable Period | Buy when price > MAVP, sell when price < MAVP |
| `MIDPOINT_indicator` | Midpoint Price | Buy when price > midpoint, sell when price < midpoint |
| `MIDPRICE_indicator` | Midprice | Buy when price > midprice, sell when price < midprice |
| `SAR_indicator` | Parabolic SAR | Buy when SAR < price, sell when SAR > price |
| `SAREXT_indicator` | Extended Parabolic SAR | Buy when SAR < price, sell when SAR > price |
| `SMA_indicator` | Simple Moving Average | Buy when price > SMA, sell when price < SMA |
| `T3_indicator` | Triple Exponential Moving Average | Buy when price > T3, sell when price < T3 |
| `TEMA_indicator` | Triple Exponential Moving Average | Buy when price > TEMA, sell when price < TEMA |
| `TRIMA_indicator` | Triangular Moving Average | Buy when price > TRIMA, sell when price < TRIMA |
| `WMA_indicator` | Weighted Moving Average | Buy when price > WMA, sell when price < WMA |
| `ICHIMOKU_indicator` | Ichimoku Cloud | Buy when price > cloud, sell when price < cloud |
| `KELTNER_indicator` | Keltner Channels | Buy when price touches lower channel, sell when touching upper channel |

### 2. Momentum Indicators (28 indicators)

These indicators measure the rate of change in price and help identify trend strength and potential reversals.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `ADX_indicator` | Average Directional Index | Buy when ADX > 25 and rising, sell when ADX < 20 |
| `ADXR_indicator` | Average Directional Index Rating | Buy when ADXR > 25, sell when ADXR < 20 |
| `APO_indicator` | Absolute Price Oscillator | Buy when APO > 0, sell when APO < 0 |
| `AROON_indicator` | Aroon | Buy when Aroon Up > Aroon Down, sell when Aroon Down > Aroon Up |
| `AROONOSC_indicator` | Aroon Oscillator | Buy when AROONOSC > 0, sell when AROONOSC < 0 |
| `BOP_indicator` | Balance of Power | Buy when BOP > 0, sell when BOP < 0 |
| `CCI_indicator` | Commodity Channel Index | Buy when CCI > 100, sell when CCI < -100 |
| `CMO_indicator` | Chande Momentum Oscillator | Buy when CMO > 0, sell when CMO < 0 |
| `DX_indicator` | Directional Movement Index | Buy when DX > 25, sell when DX < 20 |
| `MACD_indicator` | MACD | Buy when MACD > Signal, sell when MACD < Signal |
| `MACDEXT_indicator` | MACD with controllable MA type | Buy when MACD > Signal, sell when MACD < Signal |
| `MACDFIX_indicator` | MACD with fixed period | Buy when MACD > Signal, sell when MACD < Signal |
| `MFI_indicator` | Money Flow Index | Buy when MFI > 80, sell when MFI < 20 |
| `PLUS_MINUS_DI_indicator` | Plus/Minus Directional Indicator | Buy when +DI > -DI, sell when -DI > +DI |
| `MINUS_DM_indicator` | Minus Directional Movement | Buy when -DM < threshold, sell when -DM > threshold |
| `MOM_indicator` | Momentum | Buy when MOM > 0, sell when MOM < 0 |
| `PLUS_DM_indicator` | Plus Directional Movement | Buy when +DM > threshold, sell when +DM < threshold |
| `PPO_indicator` | Percentage Price Oscillator | Buy when PPO > 0, sell when PPO < 0 |
| `ROC_indicator` | Rate of Change | Buy when ROC > 0, sell when ROC < 0 |
| `ROCP_indicator` | Rate of Change Percentage | Buy when ROCP > 0, sell when ROCP < 0 |
| `ROCR_indicator` | Rate of Change Ratio | Buy when ROCR > 1, sell when ROCR < 1 |
| `ROCR100_indicator` | Rate of Change Ratio 100 Scale | Buy when ROCR100 > 100, sell when ROCR100 < 100 |
| `RSI_indicator` | Relative Strength Index | Buy when RSI < 30, sell when RSI > 70 |
| `STOCH_indicator` | Stochastic | Buy when %K < 20, sell when %K > 80 |
| `STOCHF_indicator` | Stochastic Fast | Buy when %K < 20, sell when %K > 80 |
| `STOCHRSI_indicator` | Stochastic RSI | Buy when STOCHRSI < 20, sell when STOCHRSI > 80 |
| `TRIX_indicator` | Triple Exponential Average | Buy when TRIX > 0, sell when TRIX < 0 |
| `ULTOSC_indicator` | Ultimate Oscillator | Buy when ULTOSC < 30, sell when ULTOSC > 70 |
| `WILLR_indicator` | Williams' %R | Buy when WILLR < -80, sell when WILLR > -20 |

### 3. Volume Indicators (4 indicators)

These indicators analyze trading volume to confirm price movements and identify potential reversals.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `AD_indicator` | Accumulation/Distribution | Buy when AD > 0, sell when AD < 0 |
| `ADOSC_indicator` | Accumulation/Distribution Oscillator | Buy when ADOSC > 0, sell when ADOSC < 0 |
| `OBV_indicator` | On Balance Volume | Buy when OBV > 0, sell when OBV < 0 |
| `VWAP_indicator` | Volume Weighted Average Price | Buy when price < VWAP, sell when price > VWAP |

### 4. Cycle Indicators (5 indicators)

These indicators identify cyclical patterns in price movements.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `HT_DCPERIOD_indicator` | Hilbert Transform Dominant Cycle Period | Buy when cycle period < threshold, sell when cycle period > threshold |
| `HT_DCPHASE_indicator` | Hilbert Transform Dominant Cycle Phase | Buy when phase < threshold, sell when phase > threshold |
| `HT_PHASOR_indicator` | Hilbert Transform Phasor Components | Buy when phasor > threshold, sell when phasor < threshold |
| `HT_SINE_indicator` | Hilbert Transform SineWave | Buy when sine > threshold, sell when sine < threshold |
| `HT_TRENDMODE_indicator` | Hilbert Transform Trend vs Cycle Mode | Buy when trend mode, sell when cycle mode |

### 5. Price Transforms (4 indicators)

These indicators transform price data into different representations.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `AVGPRICE_indicator` | Average Price | Buy when current price > average price, sell when current price < average price |
| `MEDPRICE_indicator` | Median Price | Buy when current price > median price, sell when current price < median price |
| `TYPPRICE_indicator` | Typical Price | Buy when current price > typical price, sell when current price < typical price |
| `WCLPRICE_indicator` | Weighted Close Price | Buy when current price > weighted close, sell when current price < weighted close |

### 6. Volatility Indicators (3 indicators)

**Warning**: These indicators are generally not recommended for direct buy/sell signals as they measure volatility rather than direction.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `ATR_indicator` | Average True Range | Buy when ATR < threshold, sell when ATR > threshold |
| `NATR_indicator` | Normalized Average True Range | Buy when NATR < threshold, sell when NATR > threshold |
| `TRANGE_indicator` | True Range | Buy when TRANGE < threshold, sell when TRANGE > threshold |

### 7. Pattern Recognition (61 indicators)

These indicators identify specific candlestick patterns that may indicate price reversals or continuations.

#### Bullish Patterns
- `CDLHAMMER_indicator` - Hammer pattern
- `CDLINVERTEDHAMMER_indicator` - Inverted Hammer
- `CDLMORNINGSTAR_indicator` - Morning Star
- `CDLMORNINGDOJISTAR_indicator` - Morning Doji Star
- `CDLPIERCING_indicator` - Piercing Pattern
- `CDLENGULFING_indicator` - Bullish Engulfing
- `CDLHARAMI_indicator` - Harami Pattern
- `CDLHARAMICROSS_indicator` - Harami Cross

#### Bearish Patterns
- `CDLHANGINGMAN_indicator` - Hanging Man
- `CDLSHOOTINGSTAR_indicator` - Shooting Star
- `CDLEVENINGSTAR_indicator` - Evening Star
- `CDLEVENINGDOJISTAR_indicator` - Evening Doji Star
- `CDLDARKCLOUDCOVER_indicator` - Dark Cloud Cover
- `CDLDOJI_indicator` - Doji
- `CDLDOJISTAR_indicator` - Doji Star

#### Continuation Patterns
- `CDLSPINNINGTOP_indicator` - Spinning Top
- `CDLMARUBOZU_indicator` - Marubozu
- `CDLLONGLINE_indicator` - Long Line Candle
- `CDLSHORTLINE_indicator` - Short Line Candle

### 8. Statistical Functions (8 indicators)

These indicators perform statistical analysis on price data.

| Indicator | Description | Signal Logic |
|-----------|-------------|--------------|
| `BETA_indicator` | Beta | Buy when beta > 1, sell when beta < 1 |
| `CORREL_indicator` | Pearson's Correlation | Buy when correlation > threshold, sell when correlation < threshold |
| `LINEARREG_ANGLE_indicator` | Linear Regression Angle | Buy when angle > 0, sell when angle < 0 |
| `LINEARREG_INTERCEPT_indicator` | Linear Regression Intercept | Buy when intercept > price, sell when intercept < price |
| `LINEARREG_SLOPE_indicator` | Linear Regression Slope | Buy when slope > 0, sell when slope < 0 |
| `STDDEV_indicator` | Standard Deviation | Buy when std dev < threshold, sell when std dev > threshold |
| `TSF_indicator` | Time Series Forecast | Buy when forecast > price, sell when forecast < price |
| `VAR_indicator` | Variance | Buy when variance < threshold, sell when variance > threshold |

## Strategy Implementation

### Vectorized Implementation

AmpyFin uses vectorized implementations of TA-Lib indicators for improved performance:

```python
def BBANDS_indicator(ticker, data):
    """Bollinger Bands indicator with vectorized calculations."""
    upper, middle, lower = ta.BBANDS(data['close'], timeperiod=20)
    
    # Vectorized signal generation
    signals = np.select([
        data['close'] <= lower,  # Buy condition
        data['close'] >= upper   # Sell condition
    ], [
        'Buy',
        'Sell'
    ], default='Hold')
    
    return signals.iloc[-1]  # Return latest signal
```

### Strategy Evaluation

Each strategy is evaluated based on:

1. **Signal Generation**: Buy/Sell/Hold signals based on technical analysis
2. **Performance Tracking**: Success/failure rates tracked in MongoDB
3. **Ranking System**: Strategies ranked by performance metrics
4. **Weighted Decisions**: Final trading decisions weighted by strategy performance

## Custom Strategy Development

### Creating a New Strategy

1. **Define the function**:
```python
def CUSTOM_indicator(ticker, data):
    """Custom trading strategy."""
    # Calculate your indicator
    custom_value = calculate_custom_indicator(data)
    
    # Generate signals
    if custom_value > threshold:
        return "Buy"
    elif custom_value < -threshold:
        return "Sell"
    else:
        return "Hold"
```

2. **Add to strategy list**:
```python
# In strategies/categorise_talib_indicators_vect.py
custom_strategies = [CUSTOM_indicator]
strategies = (
    overlap_studies
    + momentum_indicators
    + volume_indicators
    + cycle_indicators
    + price_transforms
    + volatility_indicators
    + pattern_recognition
    + statistical_functions
    + custom_strategies  # Add your custom strategies
)
```

3. **Test the strategy**:
```python
# Test with historical data
result = CUSTOM_indicator("AAPL", historical_data)
print(f"Signal: {result}")
```

### Strategy Best Practices

1. **Use vectorized operations** for better performance
2. **Handle edge cases** (insufficient data, NaN values)
3. **Test thoroughly** with different market conditions
4. **Document signal logic** clearly
5. **Consider market context** (trending vs ranging markets)

## Performance Optimization

### Indicator Periods

Each indicator has an optimal period for different market conditions:

```python
indicator_periods = {
    "BBANDS_indicator": "1y",
    "RSI_indicator": "1mo", 
    "MACD_indicator": "3mo",
    "ATR_indicator": "3mo",
    # ... more indicators
}
```

### Strategy Selection

The system automatically selects the best-performing strategies based on:

1. **Historical performance**
2. **Current market conditions**
3. **Risk-adjusted returns**
4. **Drawdown characteristics**

## Troubleshooting Strategies

### Common Issues

1. **Insufficient Data**: Ensure enough historical data for indicator calculation
2. **NaN Values**: Handle missing data appropriately
3. **Signal Conflicts**: Multiple strategies may give conflicting signals
4. **Overfitting**: Avoid optimizing for specific historical periods

### Debugging

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test individual strategy
result = RSI_indicator("AAPL", test_data)
print(f"RSI Signal: {result}")
```

## Strategy Performance Monitoring

### MongoDB Collections

- `algorithm_holdings`: Strategy portfolio values and trade statistics
- `points_tally`: Strategy performance scores
- `rank_to_coefficient`: Strategy ranking coefficients

### Weights & Biases Integration

Strategy performance is automatically tracked in Weights & Biases for:
- Performance metrics
- Strategy comparison
- Historical analysis
- Optimization insights
