# **AmpyFin Trading Bot**
![AmpyFin Logo](logo.png)

## **Introduction**

Welcome to **AmpyFin**, an advanced AI-powered trading bot designed for the NASDAQ-100. Imagine having several expert traders working for you 24/7—**AmpyFin** makes this a reality. 

Built with cutting-edge technology, AmpyFin constantly monitors market conditions, executes trades, and refines its strategies to ensure optimal performance. Whether you're an experienced trader or new to algorithmic trading, AmpyFin offers a robust, highly adaptable trading system that will elevate your trading strategies.

## **AmpyFin’s Data Collection Power**

**AmpyFin** begins its operation by tapping into the **Financial Modeling Prep API**, collecting NASDAQ-100 ticker data to gain crucial market insights. This data is used to help the bot make informed decisions and set up trades.

To stay ahead in the fast-moving world of trading, AmpyFin uses the **Polygon API** to monitor real-time market status and feed the bot with the most current market conditions. This allows AmpyFin to execute trades based on up-to-the-minute data, ensuring swift, informed decision-making.

All collected data and trading logs are securely stored in **MongoDB**, enabling quick access to trading information and providing a secure backend for historical data and analysis.

## **Algorithms at Work**

At the core of **AmpyFin** are its diverse algorithms, each designed to tackle different market conditions. The bot does not rely on just one strategy—**AmpyFin** simultaneously employs a variety of trading algorithms, each optimized for different market scenarios.

These algorithms range from fundamental strategies like **mean reversion** to more sophisticated, **AI-driven** approaches. The strategies are continually tested and refined in real-time market conditions, giving **AmpyFin** a competitive edge over traditional traders.

Some of the strategies **AmpyFin** employs include:

- **Mean Reversion**: Predicts that asset prices will eventually return to their historical average.
- **Momentum**: Capitalizes on prevailing market trends.
- **Arbitrage**: Identifies and exploits price discrepancies between related assets.

Additionally, AmpyFin leverages its own AI-driven strategies to further enhance trading performance. These algorithms work collaboratively, with each one contributing its strength to the overall system.

## **How Dynamic Ranking Works**

Managing multiple trading algorithms is no easy task. **AmpyFin** simplifies this by using a **dynamic ranking system** that ranks algorithms based on their performance—both real and simulated.

Each algorithm starts with an initial base score of **50,000** and is ranked based on profitability. The bot evaluates each algorithm's performance and assigns it a weight based on its rank. The ranking system uses a function to calculate how much influence each algorithm should have over the final trading decision. The function looks like this:

$$
\left( \frac{e^e}{e^2 - 1} \right)^{2i}
$$

Where \(i\) is the inverse of the algorithm's ranking. The dynamic nature of the ranking system ensures that the highest-performing algorithms have more influence, while underperforming algorithms lose their weight in the decision-making process. This allows **AmpyFin** to adjust in real-time to changing market conditions.

The system is designed with a **time delta coefficient**, which ensures that recent trades are given greater weight in decision-making, though it is balanced to prevent extreme bias towards any single trade.

The dynamic ranking system allows **AmpyFin** to:
- Adapt to ever-changing market conditions.
- Prioritize high-performing algorithms.
- Balance risk while maximizing potential returns.

## **API Endpoints**

- **Main API Endpoint**: [https://ampyfin-api-app.onrender.com/](https://ampyfin-api-app.onrender.com/)  
  This is the main entry point for the AmpyFin trading bot, offering access to all available endpoints.

- **Rankings Endpoint**: [https://ampyfin-api-app.onrender.com/rankings](https://ampyfin-api-app.onrender.com/rankings)  
  This GET endpoint returns the current rankings of the trading algorithms. Higher ranks indicate better performance.

- **Holdings Endpoint**: [https://ampyfin-api-app.onrender.com/holdings](https://ampyfin-api-app.onrender.com/holdings)  
  This GET endpoint provides the current holdings of the trading bot.

- **Portfolio & Major ETFs Endpoint**: [https://ampyfin-api-app.onrender.com/portfolio_percentage](https://ampyfin-api-app.onrender.com/portfolio_percentage)  
  This GET endpoint provides the current total profit percentage of the trading bot since going live. It also provides the current percentage for the benchmark QQQ and SPY etfs.

- **Test Endpoint**: [https://ampyfin-api-app.onrender.com/ticker/{ticker}](https://ampyfin-api-app.onrender.com/ticker/)  
  This GET endpoint provides the current sentiment of the trading bot on the particular ticker. Replace {ticker} with an actual ticker symbol. The ticker symbol should be in all caps. It doesn't need to be in the NDAQ-100 but must be listed in the NYSE or NASDAQ.


## **Features**

- **NASDAQ-100 Ticker Retrieval**: AmpyFin retrieves tickers using the **Financial Modeling Prep API** during early market hours.
- **Real-Time Market Monitoring**: **Polygon API** is used to track market status (open, closed, premarket) and feed the bot with up-to-date market conditions.
- **Dynamic Algorithm Ranking System**: AmpyFin adjusts its algorithm rankings based on real-time performance to prioritize the most profitable strategies.
- **Simulated Trading (Paper Trading)**: Provides a risk-free environment for testing strategies with an option to switch to live trading.
- **Data Storage**: **MongoDB** is used to securely store market data, trading logs, and algorithm performance.
- **Customizable Strategies**: Easily extendable to incorporate new trading strategies or adjust configurations based on market analysis.

## **File Structure and Objectives**

### `client.py`
- **Objective**: Manages both trading and ranking clients.
- **Features**:
  - Initiates and orchestrates trading and ranking operations.

### `trading_client.py`
- **Objective**: Executes trading based on algorithmic decisions.
- **Features**:
  - Executes trading algorithms every 60 seconds.
  - Ensures a minimum balance of $15,000 and maintains 30% liquidity.
  - Logs trades with timestamps, stock details, and reasons.
  - Validates balance and prevents unauthorized selling.

### `ranking_client.py`
- **Objective**: Runs the ranking algorithm to evaluate and rank trading strategies.
- **Features**:
  - Downloads and stores NASDAQ-100 tickers in MongoDB.
  - Executes strategies on each ticker.
  - Updates algorithm scores based on trade performance.
  - Refreshes rankings every 30 seconds.

### `trading_strategies.py`
- **Objective**: Defines various trading strategies.
- **Features**:
  - Includes strategies like mean reversion, momentum, and arbitrage.
  - Ensures consistency across strategy decision-making.

### Helper Files
- **`client_helper.py`**: Common functions for client operations (MongoDB setup, error handling).
- **`ranking_helper.py`**: Functions for updating rankings based on performance.

## **Table of Contents**
- [Installation](#installation)
- [Configuration](#configuration)
- [API Setup](#api-setup)
- [Usage](#usage)
- [Logging](#logging)
- [Notes](#notes)
- [Contributing](#contributing)
- [License](#license)

## **Installation**

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/yeonholee50/polygon-trading-bot.git
    cd polygon-trading-bot
    ```

2. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Set Up MongoDB**:
   - Sign up for a MongoDB cluster (e.g., via [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)).
   - Get your MongoDB connection string and create a database for stock data storage.
   - Replace my MongoDB string in the mongo_url variable in trading_client.py and ranking_client.py.
   - Make sure to give yourself database access for both network and database itself. For network, add your IP or add 0.0.0.0/0. For database, give yourself access to the database by creating a user and password.
   - Run initialize_rank and insert_rank_to_coefficient(100) in ranking_client.py to initialize the trading simulator.
   

## **Configuration**

1. **Create `config.py`**:
   - Copy `config_template.py` to `config.py` and enter your API keys and MongoDB credentials.
    ```python
    POLYGON_API_KEY = "your_polygon_api_key"
    FINANCIAL_PREP_API_KEY = "your_fmp_api_key"
    MONGO_DB_USER = "your_mongo_user"
    MONGO_DB_PASS = "your_mongo_password"
    API_KEY = "your_alpaca_api_key"
    API_SECRET = "your_alpaca_secret_key"
    BASE_URL = "https://paper-api.alpaca.markets"
    ```

## **API Setup**

### Polygon API
1. Sign up at [Polygon.io](https://polygon.io/) and get an API key.
2. Add it to `config.py` as `POLYGON_API_KEY`.

### Financial Modeling Prep API
1. Sign up at [Financial Modeling Prep](https://financialmodelingprep.com/) and get an API key.
2. Add it to `config.py` as `FINANCIAL_PREP_API_KEY`.

### Alpaca API
1. Sign up at [Alpaca](https://alpaca.markets/) and get API keys.
2. Add them to `config.py` as `API_KEY` and `API_SECRET`.

## Usage

To start the bot, execute on two separate terminals:
```bash
python ranking_client.py
python trading_client.py
```

## Logging

The bot logs all major events and errors to a `system.log` file, including API errors, MongoDB operations, and market status checks. You can access the log file to review the bot's activities and diagnose potential issues. The bot will also log rank events to a separate `rank.log` file.

## Notes

- The bot is limited to 250 API calls per day (as per the Polygon API free tier).
- Future enhancements can include adding custom trading strategies or integrating with a brokerage API for live trading.

## Contributing

Contributions are welcome! Feel free to open a pull request or submit issues for bugs or feature requests. Future improvements may focus on optimizing the ranking system or expanding the bot's capabilities for more advanced trading strategies. All contributions should be made on the test branch. Please do not make it on the main branch

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
