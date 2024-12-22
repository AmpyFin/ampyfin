import sys
import os
ampyfin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'AmpyFin'))
sys.path.append(ampyfin_path)
from alpaca.trading.client import TradingClient
from config import API_KEY, API_SECRET
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def sell_all_positions():
    # Initialize trading client
    trading_client = TradingClient(API_KEY, API_SECRET)
    
    try:
        # Get all open positions
        positions = trading_client.get_all_positions()
        
        if not positions:
            logging.info("No open positions found.")
            return
        
        # Close each position
        for position in positions:
            try:
                logging.info(f"Closing position for {position.symbol} (Quantity: {position.qty})")
                trading_client.close_position(position.symbol)
                logging.info(f"Successfully closed position for {position.symbol}")
            except Exception as e:
                logging.error(f"Error closing position for {position.symbol}: {e}")
        
        logging.info("All positions have been closed.")
    
    except Exception as e:
        logging.error(f"Error getting positions: {e}")

if __name__ == "__main__":
    sell_all_positions()
