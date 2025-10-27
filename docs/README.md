# AmpyFin Documentation

Welcome to the comprehensive documentation for AmpyFin, an advanced AI-powered trading system for NASDAQ-100 trading.

## 📚 Documentation Overview

This documentation provides detailed information about all aspects of the AmpyFin trading system, from initial setup to advanced configuration and troubleshooting.

## 🚀 Getting Started

### New Users
1. **[Configuration Guide](configuration.md)** - Start here for setup instructions
2. **[Troubleshooting Guide](troubleshooting.md)** - Common issues and solutions
3. **[API Integration](api-integration.md)** - Setting up broker connections

### Developers
1. **[Trading Strategies](strategies.md)** - Understanding and developing strategies
2. **[Database Schema](database-schema.md)** - Data structure and management
3. **[Configuration Guide](configuration.md)** - Advanced configuration options

## 📖 Documentation Sections

### [Configuration Guide](configuration.md)
Complete guide to configuring AmpyFin for different trading scenarios:
- Environment setup and API configuration
- Risk management parameters
- Portfolio management settings
- Strategy evaluation parameters
- Operating modes (train/test/live/push)
- Best practices and common scenarios

### [Trading Strategies](strategies.md)
Comprehensive guide to the 100+ trading strategies implemented:
- **Overlap Studies** (19 indicators): Bollinger Bands, Moving Averages, etc.
- **Momentum Indicators** (28 indicators): RSI, MACD, Stochastic, etc.
- **Volume Indicators** (4 indicators): OBV, A/D, VWAP, etc.
- **Cycle Indicators** (5 indicators): Hilbert Transform indicators
- **Price Transforms** (4 indicators): Average Price, Median Price, etc.
- **Volatility Indicators** (3 indicators): ATR, NATR, True Range
- **Pattern Recognition** (61 indicators): Candlestick patterns
- **Statistical Functions** (8 indicators): Beta, Correlation, Regression

### [Database Schema](database-schema.md)
Detailed information about data storage and management:
- **MongoDB Collections**: Trading data, strategy performance, system configuration
- **SQLite Databases**: Local price data and strategy decisions
- **Data Flow**: From collection to strategy execution
- **Database Maintenance**: Backup, cleanup, and optimization
- **Performance Tuning**: Indexing and connection management

### [API Integration](api-integration.md)
Complete guide to external service integrations:
- **Alpaca Markets**: Primary broker for trade execution
- **Interactive Brokers**: Alternative broker integration
- **yfinance**: Market data provider
- **Weights & Biases**: Experiment tracking and monitoring
- **Error Handling**: Resilience and retry logic
- **Security**: API key management and secure communication

### [Troubleshooting Guide](troubleshooting.md)
Comprehensive troubleshooting information:
- **Installation Issues**: TA-Lib, dependencies, environment setup
- **Configuration Problems**: Environment variables, API keys, MongoDB
- **Trading System Issues**: Strategy execution, portfolio management
- **Data Issues**: Missing data, synchronization problems
- **Performance Issues**: Memory usage, slow execution
- **Debugging Tools**: Logging, monitoring, health checks

## 🔧 Quick Reference

### Essential Commands
```bash
# Setup
python setup.py

# Training
python TradeSim/main.py  # mode = "train"

# Testing  
python TradeSim/main.py  # mode = "test"

# Live Trading
python TradeSim/ranking.py
python TradeSim/trading.py
```

### Key Configuration Files
- `control.py` - Main configuration file
- `.env` - Environment variables
- `config.py` - Configuration loader
- `requirements.txt` - Python dependencies

### Important Directories
- `docs/` - Documentation files
- `strategies/` - Trading strategy implementations
- `TradeSim/` - Main trading simulation modules
- `utilities/` - Helper functions and utilities
- `dbs/` - SQLite database scripts

## 🆘 Getting Help

### Before Asking for Help
1. Check the [Troubleshooting Guide](troubleshooting.md)
2. Review the relevant documentation section
3. Search existing GitHub issues
4. Check the YouTube tutorial playlist

### When Reporting Issues
Include the following information:
- **Error messages**: Complete traceback
- **Configuration**: Relevant parameters from `control.py`
- **Environment**: OS, Python version, package versions
- **Steps to reproduce**: Detailed reproduction steps
- **Log files**: System and application logs

### Community Resources
- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Questions and community support
- **YouTube Tutorials**: Video guides and walkthroughs
- **Contributing Guide**: How to contribute to the project

## 📈 System Architecture

### Core Components
- **Strategy Engine**: 100+ TA-Lib indicators with ranking system
- **Data Management**: MongoDB + SQLite hybrid storage
- **Trading Execution**: Alpaca Markets integration
- **Performance Tracking**: Weights & Biases integration
- **Risk Management**: Configurable stop-loss and take-profit

### Data Flow
1. **Data Collection**: yfinance → MongoDB
2. **Strategy Processing**: Historical data → TA-Lib indicators
3. **Ranking System**: Performance evaluation → Strategy weights
4. **Trading Decisions**: Weighted signals → Order execution
5. **Performance Tracking**: Results → Weights & Biases

## 🔒 Security Considerations

### API Security
- Use environment variables for API keys
- Implement proper error handling
- Use HTTPS/TLS for all connections
- Regular key rotation

### Trading Security
- Start with paper trading
- Implement proper risk management
- Monitor system health
- Regular backups

## 📊 Performance Optimization

### System Requirements
- **Python**: 3.8+
- **Memory**: 8GB+ recommended
- **Storage**: 10GB+ for data
- **Network**: Stable internet connection

### Optimization Tips
- Use vectorized operations for strategies
- Implement proper caching
- Monitor memory usage
- Optimize database queries
- Use parallel processing where appropriate

## 🤝 Contributing

We welcome contributions! Please see the [Contributing Guide](../CONTRIBUTING.md) for:
- Development environment setup
- Code style guidelines
- Testing procedures
- Pull request process

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](../MIT_LICENSE.txt) file for details.

---

**Happy Trading!** 🚀

For the latest updates and community discussions, visit our [GitHub repository](https://github.com/AmpyFin/ampyfin).
