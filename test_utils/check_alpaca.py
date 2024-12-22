import sys
import os
# Add AmpyFin to Python path
ampyfin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'AmpyFin'))
sys.path.append(ampyfin_path)
from alpaca.trading.client import TradingClient
from config import API_KEY, API_SECRET
import logging

logging.basicConfig(level=logging.INFO)

def check_positions():
    """
    Just check what positions exist in Alpaca
    """
    try:
        # Connect to Alpaca
        trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
        
        # Get account info
        account = trading_client.get_account()
        print(f"\nAlpaca Account Status:")
        print(f"  Cash: ${float(account.cash):,.2f}")
        print(f"  Portfolio Value: ${float(account.portfolio_value):,.2f}")
        
        # Get current positions
        print("\nCurrent Alpaca Positions:")
        positions = trading_client.get_all_positions()
        if positions:
            for position in positions:
                print(f"  {position.symbol}: {position.qty} shares @ ${float(position.avg_entry_price):.2f}")
        else:
            print("  No open positions")
            
    except Exception as e:
        logging.error(f"Error checking Alpaca positions: {e}")

if __name__ == "__main__":
    check_positions()
