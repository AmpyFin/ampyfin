# Configuration Guide

This document provides comprehensive information about configuring AmpyFin for different trading scenarios and environments.

## Overview

AmpyFin uses `control.py` as the central configuration file. This file contains all the parameters that control the behavior of the trading system, including risk management, strategy evaluation, and operational modes.

## Configuration Parameters

### General Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project_name` | string | "AmpyFin - TestRound" | Project identifier for Weights & Biases tracking |
| `experiment_name` | string | "FourthTest" | Experiment identifier for Weights & Biases tracking |
| `mode` | string | "train" | Operating mode: 'train', 'test', 'live', or 'push' |
| `benchmark_asset` | string | "QQQ" | Benchmark asset for performance comparison |

### Time Delta Configuration

The time delta system controls how historical data influences current decisions:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `time_delta_mode` | string | "additive" | Mode: 'additive', 'multiplicative', or 'balanced' |
| `time_delta_increment` | float | 0.01 | Increment for additive mode |
| `time_delta_multiplicative` | float | 1.01 | Multiplier for multiplicative mode |
| `time_delta_balanced` | float | 0.2 | Balance factor (0.8 data influence, 0.2 current influence) |

#### Time Delta Modes Explained

- **Additive**: `time_delta += time_delta_increment` - Less overfitting but potential underfitting over time
- **Multiplicative**: `time_delta *= time_delta_multiplicative` - More overfitting but less underfitting over time  
- **Balanced**: `time_delta = (1 - time_delta_balanced) * data_influence + time_delta_balanced * current_influence` - Balanced approach

### Risk Management Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stop_loss` | float | 0.03 | Stop loss percentage (3% loss triggers sell) |
| `take_profit` | float | 0.05 | Take profit percentage (5% gain triggers sell) |

### Training Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `train_period_start` | string | "2024-01-01" | Start date for training period (YYYY-MM-DD) |
| `train_period_end` | string | "2024-01-15" | End date for training period (YYYY-MM-DD) |
| `test_period_start` | string | "2024-01-16" | Start date for testing period (YYYY-MM-DD) |
| `test_period_end` | string | "2025-01-30" | End date for testing period (YYYY-MM-DD) |
| `train_tickers` | list | [] | Specific tickers to train on (empty = use NASDAQ-100) |
| `train_start_cash` | float | 50000.00 | Starting cash for training simulation |
| `train_suggestion_heap_limit` | int | 600000 | Buy weight threshold for ticker suggestions |

### Portfolio Management Parameters

#### Training Portfolio Limits

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `train_trade_liquidity_limit` | float | 15000.00 | Cash reserve during trading |
| `train_trade_asset_limit` | float | 0.4 | Max asset allocation (40% of portfolio) |
| `train_rank_liquidity_limit` | float | 15000 | Cash reserve during ranking |
| `train_rank_asset_limit` | float | 0.3 | Max asset allocation during ranking (30%) |

#### Live Trading Portfolio Limits

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `trade_liquidity_limit` | float | 15000 | Cash reserve for live trading |
| `trade_asset_limit` | float | 0.1 | Max asset allocation for live trading (10%) |
| `rank_liquidity_limit` | float | 15000 | Cash reserve during ranking |
| `rank_asset_limit` | float | 0.1 | Max asset allocation during ranking (10%) |
| `suggestion_heap_limit` | int | 600000 | Buy weight threshold for suggestions |

### Strategy Evaluation Parameters

#### Profit/Loss Rewards and Penalties

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `profit_price_change_ratio_d1` | float | 1.05 | First profit threshold (5% gain) |
| `profit_profit_time_d1` | float | 1 | Reward multiplier for first profit threshold |
| `profit_price_change_ratio_d2` | float | 1.1 | Second profit threshold (10% gain) |
| `profit_profit_time_d2` | float | 1.5 | Reward multiplier for second profit threshold |
| `profit_profit_time_else` | float | 1.2 | Reward multiplier for exceeding second threshold |
| `loss_price_change_ratio_d1` | float | 0.975 | First loss threshold (2.5% loss) |
| `loss_profit_time_d1` | float | 1 | Penalty multiplier for first loss threshold |
| `loss_price_change_ratio_d2` | float | 0.95 | Second loss threshold (5% loss) |
| `loss_profit_time_d2` | float | 1.5 | Penalty multiplier for second loss threshold |
| `loss_profit_time_else` | float | 2 | Penalty multiplier for exceeding second threshold |

## Operating Modes

### Training Mode (`mode = "train"`)
- Runs ranking system to evaluate strategies
- Updates trading simulator with new performance data
- Generates training results for analysis
- **Warning**: Training can take a very long time (hours to days)

### Testing Mode (`mode = "test"`)
- Runs trained strategies on test data
- Evaluates performance without affecting live trading
- Generates performance reports and metrics

### Live Mode (`mode = "live"`)
- **Default safe mode** - executes actual trades
- Uses current strategy rankings for trading decisions
- Connects to live market data and broker APIs

### Push Mode (`mode = "push"`)
- Pushes trained model results to MongoDB
- Updates strategy rankings in the database
- **Note**: Currently not fully implemented

## Environment Configuration

### Required Environment Variables

Create a `.env` file with the following variables:

```bash
# Alpaca Trading API
API_KEY=your_alpaca_api_key
API_SECRET=your_alpaca_secret_key
BASE_URL=https://paper-api.alpaca.markets  # Paper trading
# BASE_URL=https://api.alpaca.markets     # Live trading (USE WITH CAUTION)

# Weights & Biases
WANDB_API_KEY=your_wandb_api_key

# MongoDB
MONGO_URL=your_mongodb_connection_string
```

### API Configuration

#### Alpaca API Setup
1. Sign up at [Alpaca Markets](https://alpaca.markets/)
2. Generate API keys from your dashboard
3. **Important**: Start with paper trading (`paper-api.alpaca.markets`)
4. Only switch to live trading after thorough testing

#### Weights & Biases Setup
1. Sign up at [Weights & Biases](https://wandb.ai/)
2. Generate API key from your profile settings
3. Used for experiment tracking and performance monitoring

#### MongoDB Setup
1. **Option A**: MongoDB Atlas (recommended)
   - Create free account at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
   - Create cluster and get connection string
2. **Option B**: Local MongoDB
   - Install MongoDB Community Edition
   - Use `mongodb://localhost:27017/ampyfin`

## Configuration Best Practices

### Risk Management
- Start with conservative `stop_loss` and `take_profit` values
- Use lower `asset_limit` values for better diversification
- Always test with paper trading before live trading

### Performance Optimization
- Use shorter training periods initially to test configuration
- Monitor `time_delta` values to prevent overfitting/underfitting
- Adjust `suggestion_heap_limit` based on market conditions

### Development Workflow
1. Start with `mode = "train"` for strategy development
2. Switch to `mode = "test"` for validation
3. Use `mode = "live"` only after thorough testing
4. Use `mode = "push"` to deploy successful models

## Common Configuration Scenarios

### Conservative Trading
```python
stop_loss = 0.02  # 2% stop loss
take_profit = 0.03  # 3% take profit
trade_asset_limit = 0.05  # 5% max allocation
```

### Aggressive Trading
```python
stop_loss = 0.05  # 5% stop loss
take_profit = 0.10  # 10% take profit
trade_asset_limit = 0.20  # 20% max allocation
```

### High-Frequency Training
```python
train_period_start = "2024-01-01"
train_period_end = "2024-01-07"  # Shorter periods
train_tickers = ["AAPL", "MSFT", "GOOGL"]  # Specific tickers
```

## Troubleshooting Configuration

### Common Issues
1. **Training takes too long**: Reduce `train_period_end` or specify `train_tickers`
2. **Overfitting**: Increase `time_delta_balanced` or use `additive` mode
3. **Underfitting**: Decrease `time_delta_balanced` or use `multiplicative` mode
4. **API errors**: Check environment variables and API key validity

### Validation
- Always validate configuration with paper trading first
- Monitor Weights & Biases for performance metrics
- Check MongoDB collections for data integrity
