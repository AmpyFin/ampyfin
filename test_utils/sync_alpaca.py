import sys
import os
ampyfin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'AmpyFin'))
sys.path.append(ampyfin_path)
from alpaca.trading.client import TradingClient
from pymongo import MongoClient
import certifi
from config import API_KEY, API_SECRET, mongo_url
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def sync_positions():
    """
    Sync MongoDB trades database with actual Alpaca long positions only
    """
    try:
        # Connect to Alpaca
        trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
        
        # Connect to MongoDB
        mongo_client = MongoClient(mongo_url, tlsCAFile=certifi.where())
        db = mongo_client.trades
        
        # Get MongoDB positions
        print("\nCurrent MongoDB positions:")
        mongo_positions = {}
        for doc in db.assets_quantities.find():
            mongo_positions[doc['symbol']] = doc['quantity']
            print(f"  {doc['symbol']}: {doc['quantity']}")
        
        # Get Alpaca long positions only
        print("\nCurrent Alpaca long positions:")
        alpaca_positions = trading_client.get_all_positions()
        alpaca_holdings = {}
        for position in alpaca_positions:
            qty = float(position.qty)
            # Only include long positions
            if qty > 0:
                alpaca_holdings[position.symbol] = qty
                print(f"  {position.symbol}: {qty} shares @ ${float(position.avg_entry_price):.2f}")
        
        # Compare differences
        print("\nDifferences in long positions:")
        all_symbols = set(mongo_positions.keys()) | set(alpaca_holdings.keys())
        has_differences = False
        for symbol in sorted(all_symbols):
            mongo_qty = mongo_positions.get(symbol, 0)
            alpaca_qty = alpaca_holdings.get(symbol, 0)
            if mongo_qty != alpaca_qty:
                has_differences = True
                print(f"  {symbol}:")
                print(f"    MongoDB: {mongo_qty}")
                print(f"    Alpaca:  {alpaca_qty}")
        
        if not has_differences:
            print("  No differences found in long positions")
            return
        
        # Update MongoDB to match Alpaca long positions
        if input("\nUpdate MongoDB to match Alpaca long positions? (y/n): ").lower() == 'y':
            # Clear existing positions
            db.assets_quantities.delete_many({})
            
            # Insert new long positions from Alpaca
            for symbol, quantity in alpaca_holdings.items():
                db.assets_quantities.insert_one({
                    "symbol": symbol,
                    "quantity": quantity
                })
            
            # Get account info
            account = trading_client.get_account()
            portfolio_value = float(account.portfolio_value)
            
            # Update portfolio value
            db.portfolio_values.insert_one({
                "name": "portfolio_percentage",
                "portfolio_value": portfolio_value
            })
            
            print("\nMongoDB updated successfully with long positions")
            print(f"Portfolio Value: ${portfolio_value:,.2f}")
        else:
            print("\nSync cancelled")
        
    except Exception as e:
        logging.error(f"Error syncing positions with Alpaca: {e}")
    finally:
        mongo_client.close()

if __name__ == "__main__":
    sync_positions()
