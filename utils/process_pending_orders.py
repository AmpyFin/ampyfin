import logging
import certifi
import time
from alpaca.trading.client import TradingClient
from pymongo import MongoClient

from config import API_KEY, API_SECRET, mongo_url
from helper_files.client_helper import check_pending_orders

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("log/pending_orders.log"),
        logging.StreamHandler(),
    ],
)

def process_pending_orders():
    """
    Utility to process pending orders. Can be run separately from the main trading client.
    Checks for pending orders in MongoDB and updates them based on Alpaca API data.
    """
    logging.info("Starting pending orders processing utility...")
    
    ca = certifi.where()
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    trading_client = TradingClient(API_KEY, API_SECRET)
    
    try:
        db = mongo_client.trades
        pending_count = db.pending_orders.count_documents({})
        
        if pending_count == 0:
            logging.info("No pending orders found.")
            return
            
        logging.info(f"Found {pending_count} pending orders to process.")
        check_pending_orders(trading_client, mongo_client)
        
        # Check how many were processed
        remaining = db.pending_orders.count_documents({})
        processed = pending_count - remaining
        logging.info(f"Processed {processed} orders. {remaining} orders still pending.")
        
    except Exception as e:
        logging.error(f"Error processing pending orders: {e}")
    finally:
        mongo_client.close()
        logging.info("Finished pending orders processing.")

if __name__ == "__main__":
    process_pending_orders() 