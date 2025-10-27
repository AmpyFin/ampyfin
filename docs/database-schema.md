# Database Schema Documentation

This document provides comprehensive information about the database structure used in AmpyFin, including MongoDB collections and SQLite databases.

## Overview

AmpyFin uses a hybrid database approach:
- **MongoDB**: Primary database for trading data, strategy performance, and system configuration
- **SQLite**: Local databases for price data and strategy decisions (in `dbs/` folder)

## MongoDB Database Structure

### Database: `trading_simulator`

This database contains all trading simulation and strategy performance data.

#### Collection: `algorithm_holdings`

Stores portfolio information and trade statistics for each strategy.

```javascript
{
  "_id": ObjectId,
  "strategy": "RSI_indicator",           // Strategy name
  "holdings": {                          // Current asset holdings
    "AAPL": 100,
    "MSFT": 50
  },
  "amount_cash": 50000,                  // Available cash
  "initialized_date": ISODate,           // When strategy was initialized
  "total_trades": 150,                   // Total number of trades
  "successful_trades": 95,               // Number of profitable trades
  "neutral_trades": 10,                  // Number of break-even trades
  "failed_trades": 45,                   // Number of losing trades
  "last_updated": ISODate,               // Last update timestamp
  "portfolio_value": 75000               // Total portfolio value
}
```

#### Collection: `points_tally`

Tracks performance scores for each strategy.

```javascript
{
  "_id": ObjectId,
  "strategy": "RSI_indicator",           // Strategy name
  "total_points": 1250.5,               // Cumulative performance score
  "initialized_date": ISODate,           // When tracking started
  "last_updated": ISODate               // Last update timestamp
}
```

#### Collection: `rank_to_coefficient`

Maps strategy ranks to their weighting coefficients.

```javascript
{
  "_id": ObjectId,
  "rank": 1,                             // Strategy rank (1 = best)
  "coefficient": 0.85                    // Weighting coefficient
}
```

### Database: `market_data`

Contains market-related configuration and status information.

#### Collection: `market_status`

Tracks current market status.

```javascript
{
  "_id": "market_status_config",
  "market_status": "open"                // "open" or "closed"
}
```

### Database: `IndicatorsDatabase`

Stores configuration for technical indicators.

#### Collection: `Indicators`

Defines optimal periods for each indicator.

```javascript
{
  "_id": ObjectId,
  "indicator": "RSI_indicator",          // Indicator name
  "ideal_period": "1mo"                  // Optimal data period
}
```

### Database: `HistoricalDatabase`

Contains historical price data cache.

#### Collection: `HistoricalDatabase`

Caches historical price data for faster access.

```javascript
{
  "_id": ObjectId,
  "ticker": "AAPL",                      // Stock symbol
  "data": {                             // Price data
    "dates": ["2024-01-01", "2024-01-02"],
    "open": [150.0, 151.0],
    "high": [152.0, 153.0],
    "low": [149.0, 150.0],
    "close": [151.0, 152.0],
    "volume": [1000000, 1100000]
  },
  "last_updated": ISODate               // Cache timestamp
}
```

### Database: `trades`

Contains live trading data and position information.

#### Collection: `paper`

Stores paper trading fills and orders.

```javascript
{
  "_id": ObjectId,
  "symbol": "AAPL",                      // Stock symbol
  "side": "buy",                         // "buy" or "sell"
  "quantity": 100,                       // Number of shares
  "price": 150.50,                       // Execution price
  "timestamp": ISODate,                  // Order timestamp
  "status": "filled"                     // Order status
}
```

#### Collection: `assets_quantities`

Tracks current asset holdings.

```javascript
{
  "_id": ObjectId,
  "symbol": "AAPL",                      // Stock symbol
  "quantity": 100                        // Number of shares held
}
```

#### Collection: `assets_limit`

Defines position limits for each asset.

```javascript
{
  "_id": ObjectId,
  "symbol": "AAPL",                      // Stock symbol
  "max_quantity": 1000,                  // Maximum shares allowed
  "max_value": 150000                    // Maximum dollar value
}
```

#### Collection: `portfolio_values`

Tracks portfolio value over time.

```javascript
{
  "_id": ObjectId,
  "name": "portfolio_percentage",        // Portfolio identifier
  "portfolio_value": 100000,             // Current portfolio value
  "timestamp": ISODate                   // Value timestamp
}
```

## SQLite Database Structure

### Database: `dbs/databases/price_data.db`

Local SQLite database for storing price data.

#### Table: `price_data`

```sql
CREATE TABLE price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    UNIQUE(ticker, date)
);
```

### Database: `dbs/databases/strategy_decisions.db`

Local SQLite database for storing strategy decisions.

#### Table: `strategy_decisions`

```sql
CREATE TABLE strategy_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    strategy TEXT NOT NULL,
    date TEXT NOT NULL,
    signal TEXT NOT NULL,  -- "Buy", "Sell", "Hold"
    confidence REAL,       -- Signal confidence score
    timestamp TEXT NOT NULL,
    UNIQUE(ticker, strategy, date)
);
```

## Database Initialization

### MongoDB Initialization

The `setup.py` script initializes all MongoDB collections:

```python
# Initialize strategy holdings
def initialize_rank():
    for strategy in strategies:
        # Create algorithm_holdings document
        # Create points_tally document

# Initialize time delta configuration
def initialize_time_delta():
    # Set initial time_delta value

# Initialize market status
def initialize_market_setup():
    # Set initial market status

# Initialize indicator periods
def initialize_indicator_setup():
    # Set optimal periods for each indicator
```

### SQLite Database Creation

The `dbs/` folder contains scripts to create and populate SQLite databases:

1. **`store_price_data.py`**: Creates and populates `price_data.db`
2. **`compute_store_strategy_decisions.py`**: Creates and populates `strategy_decisions.db`

## Data Flow

### Price Data Flow

1. **Data Collection**: yfinance → MongoDB `HistoricalDatabase`
2. **Local Storage**: MongoDB → SQLite `price_data.db`
3. **Strategy Processing**: SQLite → Strategy calculations
4. **Decision Storage**: Strategy results → SQLite `strategy_decisions.db`

### Trading Data Flow

1. **Strategy Evaluation**: Historical data → Strategy signals
2. **Ranking**: Strategy performance → MongoDB `points_tally`
3. **Portfolio Management**: Trading decisions → MongoDB `algorithm_holdings`
4. **Live Trading**: MongoDB → Broker API (Alpaca/IBKR)

## Database Maintenance

### Backup Procedures

#### MongoDB Backup
```bash
# Create backup
mongodump --uri="your_mongodb_connection_string" --out=backup/

# Restore backup
mongorestore --uri="your_mongodb_connection_string" backup/
```

#### SQLite Backup
```bash
# Copy database files
cp dbs/databases/price_data.db backup/
cp dbs/databases/strategy_decisions.db backup/
```

### Data Cleanup

#### Reset Trading Collections
```python
# utilities/mongo_reset.py
def reset_trading_collections():
    # Clear paper trading data
    # Reset asset quantities
    # Clear position limits
```

#### Clean Artifacts
```python
# utilities/clean_artificats_folder.py
def clean_artifacts():
    # Remove old log files
    # Clear temporary data
    # Clean up artifacts folder
```

## Performance Optimization

### Indexing Strategy

#### MongoDB Indexes
```javascript
// algorithm_holdings collection
db.algorithm_holdings.createIndex({"strategy": 1})
db.algorithm_holdings.createIndex({"last_updated": 1})

// points_tally collection
db.points_tally.createIndex({"strategy": 1})
db.points_tally.createIndex({"total_points": -1})

// HistoricalDatabase collection
db.HistoricalDatabase.createIndex({"ticker": 1})
db.HistoricalDatabase.createIndex({"last_updated": 1})
```

#### SQLite Indexes
```sql
-- price_data table
CREATE INDEX idx_price_data_ticker_date ON price_data(ticker, date);
CREATE INDEX idx_price_data_date ON price_data(date);

-- strategy_decisions table
CREATE INDEX idx_strategy_decisions_ticker_strategy ON strategy_decisions(ticker, strategy);
CREATE INDEX idx_strategy_decisions_date ON strategy_decisions(date);
```

### Connection Management

```python
# MongoDB connection with SSL
import certifi
mongo_client = MongoClient(MONGO_URL, tlsCAFile=certifi.where())

# Connection pooling
mongo_client = MongoClient(
    MONGO_URL,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=30000
)
```

## Monitoring and Logging

### Database Health Checks

```python
def check_database_health():
    # Check MongoDB connection
    # Verify collection integrity
    # Check SQLite database integrity
    # Monitor disk usage
```

### Performance Monitoring

- **MongoDB**: Monitor query performance, connection count, memory usage
- **SQLite**: Monitor file size, query execution time, lock contention

## Troubleshooting

### Common Issues

1. **Connection Timeouts**: Increase timeout values, check network connectivity
2. **Memory Usage**: Monitor MongoDB memory consumption, optimize queries
3. **Disk Space**: Regular cleanup of old data, monitor SQLite file sizes
4. **Data Corruption**: Regular backups, integrity checks

### Debugging Queries

```python
# Enable MongoDB query logging
import logging
logging.getLogger('pymongo').setLevel(logging.DEBUG)

# Profile slow queries
db.setProfilingLevel(2, slowms=100)
```

## Security Considerations

### MongoDB Security

1. **Authentication**: Use username/password authentication
2. **Authorization**: Implement role-based access control
3. **Encryption**: Use TLS/SSL for connections
4. **Network Security**: Restrict IP access, use VPN if needed

### SQLite Security

1. **File Permissions**: Restrict access to database files
2. **Backup Security**: Encrypt backup files
3. **Access Control**: Limit file system access

## Migration and Upgrades

### Schema Migrations

```python
def migrate_database():
    # Add new fields to existing collections
    # Update data formats
    # Handle version compatibility
```

### Database Upgrades

1. **MongoDB**: Follow official upgrade procedures
2. **SQLite**: Backup and recreate databases if needed
3. **Data Migration**: Plan for data format changes
