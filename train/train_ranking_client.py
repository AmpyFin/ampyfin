import argparse
import csv
import pandas as pd
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
from pathlib import Path

from alpaca.data import StockHistoricalDataClient, StockBarsRequest, TimeFrame

import ranking_client
from pymongo import MongoClient

import logging

from config import FINANCIAL_PREP_API_KEY, mongo_url, API_KEY, API_SECRET
from train.train_config import Config
from helper_files.client_helper import get_ndaq_tickers

def get_last_line(file, how_many_last_lines=1):
    # open your file using with: safety first, kids!
    with open(file, 'r') as file:

        # find the position of the end of the file: end of the file stream
        end_of_file = file.seek(0, 2)

        # set your stream at the end: seek the final position of the file
        file.seek(end_of_file)

        # trace back each character of your file in a loop
        n = 0
        for num in range(end_of_file + 1):
            file.seek(end_of_file - num)

            # save the last characters of your file as a string: last_line
            last_line = file.read()

            # count how many '\n' you have in your string:
            # if you have 1, you are in the last line; if you have 2, you have the two last lines
            if last_line.count('\n') == how_many_last_lines:
                return last_line

def read_csv(file_path, max_lines=None, last_line=False):
    """Reads and returns the content of a CSV file as a list of rows.
        args:
            max_lines: only return max_lines lines per csv
            last_line: if max_lines is set, this flag controls if the last line should be included as well
    """
    try:
        with open(file_path, mode='r', newline='') as file:
            reader = csv.reader(file)
            data = [row for idx, row in enumerate(reader) if (max_lines is not None and idx <= max_lines) or (max_lines is None)]  # Store all rows in a list
            if not max_lines is None and not last_line is None:
                data.append(get_last_line(file_path, how_many_last_lines=2)[1:-1].split(','))
        return data
    except FileNotFoundError:
        # logging.error(f"Error: The file '{file_path}' was not found.")
        return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

def initialize_market_status(mongo_client, date):
    status = None
    if Config.is_stock_exchange_open(date):
        status = "open"
    elif date.time() < Config.STOCK_EXCHANGE_OPENING_TIMESTAMP:
        status = "early_hours"
    else:
        status = "closed"
    logging.info(f"Initialize market status to '{status}'")
    mongo_client.market_data.market_status.update_one({}, {"$set": {"market_status": status}})

def main(args):
    """
    main function of the script.
    Args:
        args (argparse.Namespace): argparse arguments.
    """

    mongo_client = MongoClient(mongo_url)
    stock_client=StockHistoricalDataClient(api_key=API_KEY,secret_key=API_SECRET)

    Config.TRAINING = True

    logging.info(f"Checking Symbols ...")
    if args.symbols:
        symbols = read_csv(args.symbols)[0]
    else:
        symbols =  get_ndaq_tickers(mongo_client, FINANCIAL_PREP_API_KEY)

        filename = "./data/symbols.csv"
        Path("/".join(filename.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
        # Write the list to a CSV file
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows([symbols])
    Config.TRAINING_DATA.ticker = symbols


    data_prefix = 'data/historical'
    if args.historical:
        data_prefix = args.historical
    logging.info(f"Checking Historical Intraday Data ...")

    startdate = datetime.now() - Config.TRAINING_DATA_START

    with tqdm(total=len(symbols), desc="Checking Data...") as pbar:
        for i in range(len(symbols)):
            ticker = symbols[i]
            filename = f"{data_prefix}/historical_{ticker}_min.csv"
            data = None
            while data is None:
                pbar.set_description("Reading csv for %s" % ticker)
                data = read_csv(filename, max_lines=3, last_line=True)
                if data is not None:
                    data = pd.DataFrame(data[1:], columns=data[0])
                    data['timestamp'] = pd.to_datetime(data['timestamp'])
                    data = data.set_index(['symbol', 'timestamp'])
                    # sometimes intraday data from alpaca is not always 1 min so we check here only if it is not daily data
                    if (data.index[1][1] - data.index[0][1]).seconds < 24*60*60:
                        # set the start time for the training to the earliest possible data from training data
                        if data.index[0][1] < Config.CURRENT_TRAINING_TIMESTAMP:
                            Config.CURRENT_TRAINING_TIMESTAMP = data.index[0][1]
                            # set the end time for the training to the latest possible data from training data
                        if data.index[-1][1] > Config.TRAINING_END_TIMESTAMP:
                            Config.TRAINING_END_TIMESTAMP = data.index[-1][1]
                        # if we have intraday data available already we do not need to download again
                        continue
                pbar.set_description("Downloading %s" % ticker)
                sbr = StockBarsRequest(symbol_or_symbols=[ticker], timeframe=TimeFrame.Minute, start=startdate)
                try:
                    bars = stock_client.get_stock_bars(sbr)
                    data = bars.df
                except Exception as exception:
                    print(exception)
                    pbar.update(1)
                    continue
                # Renaming the columns as yf has other column names
                data = data.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
                Path("/".join(filename.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
                data.to_csv(filename)
            # set the start time for the training to the earliest possible data from training data
            if data.index[0][1] < Config.CURRENT_TRAINING_TIMESTAMP:
                Config.CURRENT_TRAINING_TIMESTAMP = data.index[0][1]
            # set the end time for the training to the latest possible data from training data
            if data.index[-1][1] > Config.TRAINING_END_TIMESTAMP:
                Config.TRAINING_END_TIMESTAMP = data.index[-1][1]
            pbar.update(1)
    logging.info(f"Checking Historical Daily Data ...")

    with tqdm(total=len(symbols), desc="Checking Data...") as pbar:
        for i in range(len(symbols)):
            pbar.set_description("Reading csv for %s" % ticker)
            ticker = symbols[i]
            filename = f"{data_prefix}/historical_{ticker}_day.csv"
            data = read_csv(filename, max_lines=3)
            if (data is not None):
                # if we have data available already we do not need to download again
                pbar.update(1)
                continue
            pbar.set_description("Downloading %s" % ticker)
            sbr = StockBarsRequest(symbol_or_symbols=[ticker], timeframe=TimeFrame.Day, start=startdate)
            bars = stock_client.get_stock_bars(sbr)
            data = bars.df
            # Renaming the columns as yf has other column names
            data = data.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
            Path("/".join(filename.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
            data.to_csv(filename)
            pbar.update(1)

    # add start offset to have enough history for the beginning
    Config.CURRENT_TRAINING_TIMESTAMP = Config.CURRENT_TRAINING_TIMESTAMP + Config.TRAINING_TIMESTAMP_START_OFFSET
    # skip weekends
    while Config.CURRENT_TRAINING_TIMESTAMP.weekday() > 4:
        Config.CURRENT_TRAINING_TIMESTAMP = Config.CURRENT_TRAINING_TIMESTAMP + timedelta(days=1)

    # data checks
    if Config.CURRENT_TRAINING_TIMESTAMP > Config.TRAINING_END_TIMESTAMP:
        raise RuntimeError("CURRENT_TRAINING_TIMESTAMP > TRAINING_END_TIMESTAMP")

    initialize_market_status(mongo_client, Config.CURRENT_TRAINING_TIMESTAMP)
    ranking_client.main()
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download/Check data and train the ranking client ",
    )

    # Arguments
    parser.add_argument(
        "-s", "--symbols",
        type=str,
        help="Path to csv containing stock symbols."
    )
    parser.add_argument(
        "-hist", "--historical",
        type=str,
        help="Path to a directory of csv's containing historical data per ticker."
    )

    # Parse Arguments
    args = parser.parse_args()

    main(args)