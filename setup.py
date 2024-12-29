from config import API_KEY, API_SECRET, POLYGON_API_KEY, MONGO_DB_USER, MONGO_DB_PASS, mongo_url
from helper_files.client_helper import strategies
from pymongo import MongoClient
from datetime import datetime
import math
import yfinance as yf
from helper_files.client_helper import get_latest_price
from alpaca.trading.client import TradingClient
from alpaca.data.historical.stock import StockHistoricalDataClient

def insert_rank_to_coefficient(i):
   try:
      client = MongoClient(mongo_url)
      db = client.trading_simulator 
      collections  = db.rank_to_coefficient
      """
      clear all collections entry first and then insert from 1 to i
      """
      collections.delete_many({})
      for i in range(1, i + 1):
      
         e = math.e
         rate = ((e**e)/(e**2) - 1)
         coefficient = rate**(2 * i)
         collections.insert_one(
            {"rank": i, 
            "coefficient": coefficient
            }
         )
      client.close()
      print("Successfully inserted rank to coefficient")
   except Exception as exception:
      print(exception)
   
  
def initialize_rank():  
   try:
      client = MongoClient(mongo_url)  
      db = client.trading_simulator  
      collections = db.algorithm_holdings  
         
      initialization_date = datetime.now()  


      for strategy in strategies:
         strategy_name = strategy.__name__


         collections = db.algorithm_holdings 
         
         if not collections.find_one({"strategy": strategy_name}):
            
            collections.insert_one({  
               "strategy": strategy_name,  
               "holdings": {},  
               "amount_cash": 50000,  
               "initialized_date": initialization_date,  
               "total_trades": 0,  
               "successful_trades": 0,
               "neutral_trades": 0,
               "failed_trades": 0,   
               "last_updated": initialization_date, 
               "portfolio_value": 50000 
            })  
         
            collections = db.points_tally  
            collections.insert_one({  
               "strategy": strategy_name,  
               "total_points": 0,  
               "initialized_date": initialization_date,  
               "last_updated": initialization_date  
            })  
               
         
      client.close()
      print("Successfully initialized rank")
   except Exception as exception:
      print(exception)

def initialize_time_delta():
   try:
      client = MongoClient(mongo_url)
      db = client.trading_simulator
      collection = db.time_delta
      collection.insert_one({"time_delta": 0.01})
      client.close()
      print("Successfully initialized time delta")
   except Exception as exception:
      print(exception)

def initialize_market_setup():
   try:
      client = MongoClient(mongo_url)
      db = client.market_data
      collection = db.market_status
      collection.insert_one({"market_status": "closed"})
      client.close()
      print("Successfully initialized market setup")
   except Exception as exception:
      print(exception)

def initialize_portfolio_percentages():
   try:
      client = MongoClient(mongo_url)
      stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)
      trading_client = TradingClient(API_KEY, API_SECRET)
      account = trading_client.get_account()
      db = client.trades
      collection = db.portfolio_values
      portfolio_value = float(account.portfolio_value)
      collection.insert_one({
         "name" : "portfolio_percentage",
         "portfolio_value": (portfolio_value-50000)/50000,
      })
      collection.insert_one({
         "name" : "ndaq_percentage",
         "portfolio_value": (get_latest_price('QQQ',stock_client)-503.17)/503.17,
      })
      collection.insert_one({
         "name" : "spy_percentage",
         "portfolio_value": (get_latest_price('SPY',stock_client)-590.50)/590.50,
      })
      client.close()
      print("Successfully initialized portfolio percentages")
   except Exception as exception:
      print(exception)

def initialize_indicator_setup():
   indicator_periods = {
    "BBANDS_indicator": "1y",
    "DEMA_indicator": "1mo",
    "EMA_indicator": "1mo",
    "HT_TRENDLINE_indicator": "6mo",
    "KAMA_indicator": "1mo",
    "MA_indicator": "3mo",
    "MAMA_indicator": "6mo",
    "MAVP_indicator": "3mo",
    "MIDPOINT_indicator": "1mo",
    "MIDPRICE_indicator": "1mo",
    "SAR_indicator": "6mo",
    "SAREXT_indicator": "6mo",
    "SMA_indicator": "1mo",
    "T3_indicator": "1mo",
    "TEMA_indicator": "1mo",
    "TRIMA_indicator": "1mo",
    "WMA_indicator": "1mo",
    "ADX_indicator": "3mo",
    "ADXR_indicator": "3mo",
    "APO_indicator": "1mo",
    "AROON_indicator": "3mo",
    "AROONOSC_indicator": "3mo",
    "BOP_indicator": "1mo",
    "CCI_indicator": "1mo",
    "CMO_indicator": "1mo",
    "DX_indicator": "1mo",
    "MACD_indicator": "3mo",
    "MACDEXT_indicator": "3mo",
    "MACDFIX_indicator": "3mo",
    "MFI_indicator": "1mo",
    "MINUS_DI_indicator": "1mo",
    "MINUS_DM_indicator": "1mo",
    "MOM_indicator": "1mo",
    "PLUS_DI_indicator": "1mo",
    "PLUS_DM_indicator": "1mo",
    "PPO_indicator": "1mo",
    "ROC_indicator": "1mo",
    "ROCP_indicator": "1mo",
    "ROCR_indicator": "1mo",
    "ROCR100_indicator": "1mo",
    "RSI_indicator": "1mo",
    "STOCH_indicator": "1mo",
    "STOCHF_indicator": "1mo",
    "STOCHRSI_indicator": "1mo",
    "TRIX_indicator": "1mo",
    "ULTOSC_indicator": "6mo",
    "WILLR_indicator": "1mo",
    "AD_indicator": "1mo",
    "ADOSC_indicator": "1mo",
    "OBV_indicator": "1mo",
    "HT_DCPERIOD_indicator": "2y",
    "HT_DCPHASE_indicator": "2y",
    "HT_PHASOR_indicator": "2y",
    "HT_SINE_indicator": "2y",
    "HT_TRENDMODE_indicator": "2y",
    "AVGPRICE_indicator": "1mo",
    "MEDPRICE_indicator": "1mo",
    "TYPPRICE_indicator": "1mo",
    "WCLPRICE_indicator": "1mo",
    "ATR_indicator": "3mo",
    "NATR_indicator": "3mo",
    "TRANGE_indicator": "3mo",
    "CDL2CROWS_indicator": "1mo",
    "CDL3BLACKCROWS_indicator": "1mo",
    "CDL3INSIDE_indicator": "1mo",
    "CDL3LINESTRIKE_indicator": "1mo",
    "CDL3OUTSIDE_indicator": "1mo",
    "CDL3STARSINSOUTH_indicator": "1mo",
    "CDL3WHITESOLDIERS_indicator": "1mo",
    "CDLABANDONEDBABY_indicator": "1mo",
    "CDLADVANCEBLOCK_indicator": "1mo",
    "CDLBELTHOLD_indicator": "1mo",
    "CDLBREAKAWAY_indicator": "1mo",
    "CDLCLOSINGMARUBOZU_indicator": "1mo",
    "CDLCONCEALBABYSWALL_indicator": "1mo",
    "CDLCOUNTERATTACK_indicator": "1mo",
    "CDLDARKCLOUDCOVER_indicator": "1mo",
    "CDLDOJI_indicator": "1mo",
    "CDLDOJISTAR_indicator": "1mo",
    "CDLDRAGONFLYDOJI_indicator": "1mo",
    "CDLENGULFING_indicator": "1mo",
    "CDLEVENINGDOJISTAR_indicator": "1mo",
    "CDLEVENINGSTAR_indicator": "1mo",
    "CDLGAPSIDESIDEWHITE_indicator": "1mo",
    "CDLGRAVESTONEDOJI_indicator": "1mo",
    "CDLHAMMER_indicator": "1mo",
    "CDLHANGINGMAN_indicator": "1mo",
    "CDLHARAMI_indicator": "1mo",
    "CDLHARAMICROSS_indicator": "1mo",
    "CDLHIGHWAVE_indicator": "1mo",
    "CDLHIKKAKE_indicator": "1mo",
    "CDLHIKKAKEMOD_indicator": "1mo",
    "CDLHOMINGPIGEON_indicator": "1mo",
    "CDLIDENTICAL3CROWS_indicator": "1mo",
    "CDLINNECK_indicator": "1mo",
    "CDLINVERTEDHAMMER_indicator": "1mo",
    "CDLKICKING_indicator": "1mo",
    "CDLKICKINGBYLENGTH_indicator": "1mo",
    "CDLLADDERBOTTOM_indicator": "1mo",
    "CDLLONGLEGGEDDOJI_indicator": "1mo",
    "CDLLONGLINE_indicator": "1mo",
    "CDLMARUBOZU_indicator": "1mo",
    "CDLMATCHINGLOW_indicator": "1mo",
    "CDLMATHOLD_indicator": "1mo",
    "CDLMORNINGDOJISTAR_indicator": "1mo",
    "CDLMORNINGSTAR_indicator": "1mo",
    "CDLONNECK_indicator": "1mo",
    "CDLPIERCING_indicator": "1mo",
    "CDLRICKSHAWMAN_indicator": "1mo",
    "CDLRISEFALL3METHODS_indicator": "1mo",
    "CDLSEPARATINGLINES_indicator": "1mo",
    "CDLSHOOTINGSTAR_indicator": "1mo",
    "CDLSHORTLINE_indicator": "1mo",
    "CDLSPINNINGTOP_indicator": "1mo",
    "CDLSTALLEDPATTERN_indicator": "1mo",
    "CDLSTICKSANDWICH_indicator": "1mo",
    "CDLTAKURI_indicator": "1mo",
    "CDLTASUKIGAP_indicator": "1mo",
    "CDLTHRUSTING_indicator": "1mo",
    "CDLTRISTAR_indicator": "1mo",
    "CDLUNIQUE3RIVER_indicator": "1mo",
    "CDLUPSIDEGAP2CROWS_indicator": "1mo",
    "CDLXSIDEGAP3METHODS_indicator": "1mo",
    "BETA_indicator": "1y",
    "CORREL_indicator": "1y",
    "LINEARREG_indicator": "2y",
    "LINEARREG_ANGLE_indicator": "2y",
    "LINEARREG_INTERCEPT_indicator": "2y",
    "LINEARREG_SLOPE_indicator": "2y",
    "STDDEV_indicator": "1mo",
    "TSF_indicator": "2y",
    "VAR_indicator": "2y",
   }
   try:
      client = MongoClient(mongo_url)
      db = client["IndicatorsDatabase"]
      collection = db["Indicators"]

      # Insert indicators into the collection
      for indicator, period in indicator_periods.items():
         collection.insert_one({"indicator": indicator, "ideal_period": period})

      print("Indicators and their ideal periods have been inserted into MongoDB.")
   except Exception as e:
      print(e)
      return

def initialize_historical_database_cache():
   try:
      client = MongoClient(mongo_url)
      db = client["HistoricalDatabase"]
      collection = db["HistoricalDatabase"]
   except:
      print("Error initializing historical database cache")
      return

if __name__ == "__main__":
   
   insert_rank_to_coefficient(200)
   
   initialize_rank()
   
   initialize_time_delta()
   
   initialize_market_setup()
   
   initialize_portfolio_percentages()

   initialize_indicator_setup()
   
   initialize_historical_database_cache()