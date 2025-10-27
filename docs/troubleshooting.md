# Troubleshooting Guide

This document provides comprehensive troubleshooting information for common issues encountered when using AmpyFin.

## Quick Diagnosis Checklist

Before diving into specific issues, run through this checklist:

- [ ] Are all environment variables set correctly?
- [ ] Is MongoDB running and accessible?
- [ ] Are API keys valid and have proper permissions?
- [ ] Is TA-Lib properly installed?
- [ ] Are all Python dependencies installed?
- [ ] Is the system running during market hours (for live trading)?

## Installation Issues

### TA-Lib Installation Problems

#### Windows
**Problem**: `Microsoft Visual C++ 14.0 is required` error
```bash
error: Microsoft Visual C++ 14.0 is required. Get it with "Microsoft Visual C++ Build Tools"
```

**Solution**:
1. Download pre-built wheel from [cgohlke/talib-build](https://github.com/cgohlke/talib-build/releases)
2. Install the appropriate wheel for your Python version:
```bash
pip install TA_Lib-0.4.24-cp39-cp39-win_amd64.whl
```

**Alternative Solution**:
1. Install Visual Studio Build Tools
2. Install TA-Lib from source:
```bash
pip install TA-Lib
```

#### macOS
**Problem**: `clang: error: unsupported option '-fopenmp'`
```bash
clang: error: unsupported option '-fopenmp'
```

**Solution**:
1. Install TA-Lib using Homebrew:
```bash
brew install ta-lib
pip install TA-Lib
```

2. If using conda:
```bash
conda install -c conda-forge ta-lib
```

#### Linux
**Problem**: `fatal error: ta-lib/ta_defs.h: No such file or directory`
```bash
fatal error: ta-lib/ta_defs.h: No such file or directory
```

**Solution**:
1. Install TA-Lib development libraries:
```bash
# Ubuntu/Debian
sudo apt-get install libta-lib-dev

# CentOS/RHEL
sudo yum install ta-lib-devel

# Then install Python package
pip install TA-Lib
```

### Python Dependencies Issues

#### Problem: Package conflicts
```bash
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed
```

**Solution**:
1. Create a virtual environment:
```bash
python -m venv ampyfin_env
source ampyfin_env/bin/activate  # Linux/Mac
# or
ampyfin_env\Scripts\activate  # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

#### Problem: Version conflicts
```bash
ERROR: Could not find a version that satisfies the requirement
```

**Solution**:
1. Update pip:
```bash
pip install --upgrade pip
```

2. Install specific versions:
```bash
pip install pandas==1.5.3 numpy==1.24.3
```

## Configuration Issues

### Environment Variables

#### Problem: Missing environment variables
```bash
[error]: Missing required environment variables: API_KEY, API_SECRET
```

**Solution**:
1. Create `.env` file in project root:
```bash
API_KEY=your_alpaca_api_key
API_SECRET=your_alpaca_secret_key
BASE_URL=https://paper-api.alpaca.markets
WANDB_API_KEY=your_wandb_api_key
MONGO_URL=your_mongodb_connection_string
```

2. Verify variables are loaded:
```python
from config import API_KEY, API_SECRET
print(f"API_KEY loaded: {bool(API_KEY)}")
```

#### Problem: Invalid API keys
```bash
alpaca.common.exceptions.APIError: 401 Unauthorized
```

**Solution**:
1. Verify API keys in Alpaca dashboard
2. Check key permissions (trading enabled)
3. Ensure using correct environment (paper vs live)
4. Test API connection:
```python
from alpaca.trading.client import TradingClient
from config import API_KEY, API_SECRET, BASE_URL

try:
    client = TradingClient(API_KEY, API_SECRET, paper=True)
    account = client.get_account()
    print("API connection successful")
except Exception as e:
    print(f"API connection failed: {e}")
```

### MongoDB Connection Issues

#### Problem: Connection refused
```bash
pymongo.errors.ServerSelectionTimeoutError: localhost:27017: [Errno 61] Connection refused
```

**Solution**:
1. **Local MongoDB**:
```bash
# Start MongoDB service
sudo systemctl start mongod  # Linux
brew services start mongodb-community  # macOS
net start MongoDB  # Windows
```

2. **MongoDB Atlas**:
   - Check connection string format
   - Verify IP whitelist
   - Check username/password

3. **Test connection**:
```python
from pymongo import MongoClient
from config import MONGO_URL

try:
    client = MongoClient(MONGO_URL)
    client.admin.command('ping')
    print("MongoDB connection successful")
except Exception as e:
    print(f"MongoDB connection failed: {e}")
```

#### Problem: Authentication failed
```bash
pymongo.errors.OperationFailure: Authentication failed
```

**Solution**:
1. Verify username and password
2. Check database permissions
3. Ensure user has read/write access
4. Test with MongoDB Compass or mongo shell

## Trading System Issues

### Strategy Execution Problems

#### Problem: No trading signals generated
```bash
No buy/sell signals generated for any ticker
```

**Debugging Steps**:
1. Check if market is open:
```python
from utilities.ranking_trading_utils import market_status
print(f"Market status: {market_status()}")
```

2. Verify data availability:
```python
from strategies.talib_indicators import get_data
data = get_data("AAPL", mongo_client, period="1mo")
print(f"Data shape: {data.shape}")
print(f"Latest data: {data.tail()}")
```

3. Test individual strategies:
```python
from strategies.talib_indicators import RSI_indicator
signal = RSI_indicator("AAPL", data)
print(f"RSI signal: {signal}")
```

#### Problem: Strategies returning NaN values
```bash
ValueError: Input contains NaN, infinity or a value too large for dtype('float64')
```

**Solution**:
1. Check data quality:
```python
import pandas as pd
print(f"NaN values: {data.isnull().sum()}")
print(f"Infinite values: {np.isinf(data).sum()}")
```

2. Clean data:
```python
# Remove NaN values
data = data.dropna()

# Replace infinite values
data = data.replace([np.inf, -np.inf], np.nan).dropna()
```

3. Add data validation to strategies:
```python
def safe_strategy(data):
    if data.isnull().any().any():
        return "Hold"
    if np.isinf(data).any().any():
        return "Hold"
    # Continue with strategy logic
```

### Portfolio Management Issues

#### Problem: Insufficient buying power
```bash
alpaca.common.exceptions.APIError: 422 Insufficient buying power
```

**Solution**:
1. Check account balance:
```python
account = trading_client.get_account()
print(f"Cash: ${float(account.cash)}")
print(f"Buying power: ${float(account.buying_power)}")
```

2. Adjust position sizes:
```python
# Reduce trade size
max_investment = min(account_cash * 0.1, 1000)  # 10% or $1000 max
```

3. Check margin requirements:
```python
# Ensure sufficient margin
if account_cash < required_margin:
    return "Hold"  # Skip trade
```

#### Problem: Order execution failures
```bash
alpaca.common.exceptions.APIError: 422 Order rejected
```

**Solution**:
1. Check order parameters:
```python
# Validate order parameters
if quantity <= 0:
    return None
if price <= 0:
    return None
```

2. Check market hours:
```python
from alpaca.trading.client import TradingClient
clock = trading_client.get_clock()
print(f"Market open: {clock.is_open}")
```

3. Use appropriate order types:
```python
# Use market orders for immediate execution
order = trading_client.submit_order(
    symbol=symbol,
    qty=quantity,
    side=side,
    type="market",  # Market order
    time_in_force="day"
)
```

## Data Issues

### Price Data Problems

#### Problem: Missing historical data
```bash
No data available for ticker: XYZ
```

**Solution**:
1. Check ticker validity:
```python
import yfinance as yf
ticker = yf.Ticker("XYZ")
info = ticker.info
if not info:
    print("Invalid ticker symbol")
```

2. Try alternative data sources:
```python
# Use different period
data = stock.history(period="6mo")  # Instead of "1y"

# Use different interval
data = stock.history(period="1y", interval="1d")  # Daily instead of minute
```

3. Handle missing data gracefully:
```python
def get_data_with_fallback(ticker, mongo_client):
    try:
        data = get_data(ticker, mongo_client)
        if data.empty:
            # Try yfinance directly
            stock = yf.Ticker(ticker)
            data = stock.history(period="1y")
        return data
    except Exception as e:
        print(f"Failed to get data for {ticker}: {e}")
        return None
```

#### Problem: Data synchronization issues
```bash
MongoDB and Alpaca positions are out of sync
```

**Solution**:
1. Use sync utility:
```python
from utilities.alpaca_utils.sync_alpaca import sync_positions
sync_positions()
```

2. Manual sync:
```python
# Get Alpaca positions
alpaca_positions = trading_client.get_all_positions()

# Update MongoDB
for position in alpaca_positions:
    db.assets_quantities.update_one(
        {"symbol": position.symbol},
        {"$set": {"quantity": float(position.qty)}},
        upsert=True
    )
```

## Performance Issues

### Memory Usage Problems

#### Problem: High memory consumption
```bash
MemoryError: Unable to allocate array
```

**Solution**:
1. Monitor memory usage:
```python
import psutil
import os

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB

print(f"Memory usage: {get_memory_usage():.2f} MB")
```

2. Optimize data processing:
```python
# Process data in chunks
chunk_size = 1000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    process_chunk(chunk)
```

3. Clear unused variables:
```python
import gc
# After processing large datasets
del large_dataframe
gc.collect()
```

### Slow Performance

#### Problem: Slow strategy execution
```bash
Strategy execution taking too long
```

**Solution**:
1. Use vectorized operations:
```python
# Instead of loops
for i in range(len(data)):
    if data.iloc[i]['close'] > data.iloc[i]['sma']:
        signals[i] = 'Buy'

# Use vectorized operations
signals = np.where(data['close'] > data['sma'], 'Buy', 'Sell')
```

2. Cache frequently used data:
```python
from functools import lru_cache

@lru_cache(maxsize=128)
def get_cached_data(ticker, period):
    return get_data(ticker, mongo_client, period)
```

3. Parallel processing:
```python
from concurrent.futures import ThreadPoolExecutor

def process_tickers_parallel(tickers):
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_ticker, tickers)
    return list(results)
```

## Logging and Debugging

### Enable Debug Logging

#### Problem: Insufficient logging information
```bash
No detailed error information available
```

**Solution**:
1. Enable debug logging:
```python
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

2. Enable specific logger:
```python
logging.getLogger('alpaca').setLevel(logging.DEBUG)
logging.getLogger('pymongo').setLevel(logging.DEBUG)
```

3. Use structured logging:
```python
import structlog

logger = structlog.get_logger()
logger.info("Processing ticker", ticker="AAPL", signal="Buy")
```

### Error Tracking

#### Problem: Errors not being caught
```bash
Unhandled exception causing system crash
```

**Solution**:
1. Add comprehensive error handling:
```python
def safe_execute(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
        return None
```

2. Use try-except blocks:
```python
try:
    result = risky_operation()
except SpecificException as e:
    logger.error(f"Specific error: {e}")
    handle_specific_error(e)
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    handle_generic_error(e)
```

## System Monitoring

### Health Checks

#### Problem: System running but not trading
```bash
System appears to be running but no trades executed
```

**Solution**:
1. Implement health checks:
```python
def system_health_check():
    checks = {
        'mongodb': check_mongodb_connection(),
        'alpaca': check_alpaca_connection(),
        'data': check_data_availability(),
        'strategies': check_strategy_execution()
    }
    
    for check, status in checks.items():
        if not status:
            logger.error(f"Health check failed: {check}")
    
    return all(checks.values())
```

2. Monitor key metrics:
```python
def monitor_system_metrics():
    metrics = {
        'cpu_usage': psutil.cpu_percent(),
        'memory_usage': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent,
        'api_calls': get_api_call_count(),
        'trades_executed': get_trade_count()
    }
    
    logger.info("System metrics", **metrics)
    return metrics
```

### Alerting

#### Problem: Issues not detected promptly
```bash
System issues not being reported
```

**Solution**:
1. Set up alerts:
```python
def send_alert(message, level="ERROR"):
    if level == "ERROR":
        # Send email/SMS notification
        send_notification(message)
    
    # Log to file
    logger.error(message)
    
    # Send to monitoring service
    send_to_monitoring_service(message)
```

2. Implement circuit breakers:
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise e
```

## Getting Help

### Log Analysis

When reporting issues, include:
1. **Error messages**: Complete error traceback
2. **Configuration**: Relevant config parameters
3. **Log files**: System and application logs
4. **Environment**: OS, Python version, package versions
5. **Steps to reproduce**: Detailed reproduction steps

### Useful Commands

#### System Information
```bash
# Python version
python --version

# Package versions
pip list

# System resources
top  # Linux/Mac
tasklist  # Windows

# Disk space
df -h  # Linux/Mac
dir  # Windows
```

#### Database Information
```bash
# MongoDB status
mongosh --eval "db.runCommand('ping')"

# MongoDB collections
mongosh --eval "db.getCollectionNames()"

# SQLite database info
sqlite3 dbs/databases/price_data.db ".schema"
```

#### Network Connectivity
```bash
# Test API connectivity
curl -I https://paper-api.alpaca.markets/v2/account

# Test MongoDB connectivity
telnet your-mongodb-host 27017
```

### Community Support

1. **GitHub Issues**: Report bugs and feature requests
2. **Discussions**: Ask questions and share experiences
3. **Documentation**: Check existing documentation first
4. **Code Review**: Review similar issues and solutions

Remember to always test solutions in a safe environment (paper trading) before applying to live trading systems.
