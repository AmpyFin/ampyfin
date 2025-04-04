import yfinance as yf
from ib_insync import IB, LimitOrder, Stock

# Constants
HOST = "127.0.0.1"
PORT = 4002  # or 7497 for paper
CLIENT_ID = 1


MAX_ORDER_VALUE = 99999  # IBKR precaution: stay under $100,000


def reset_account(ib):
    """Cancel open orders and liquidate positions in chunks using yfinance prices."""

    # Cancel open orders
    open_orders = ib.openOrders()
    for order in open_orders:
        ib.cancelOrder(order)
    print(f"✅ Canceled {len(open_orders)} open orders.")

    # Close all positions
    positions = ib.positions()
    for pos in positions:
        symbol = pos.contract.symbol
        position_size = abs(pos.position)
        action = "SELL" if pos.position > 0 else "BUY"
        contract = Stock(symbol, "SMART", "USD")

        try:
            # Fetch price from yfinance
            ticker = yf.Ticker(symbol)
            last_price = ticker.history(period="1d")["Close"].iloc[-1]
            buffer = last_price * 0.01  # 1% price buffer
            limit_price = (
                last_price - buffer if action == "SELL" else last_price + buffer
            )
        except Exception as e:
            print(f"⚠️ Error fetching price for {symbol}: {e}. Using fallback.")
            last_price = 1.0
            limit_price = 0.01 if action == "SELL" else 9999.99

        max_chunk_size = int(MAX_ORDER_VALUE / last_price)
        shares_remaining = position_size

        print(f"\n🔻 Closing {position_size} shares of {symbol} in chunks...")

        while shares_remaining > 0:
            chunk_size = min(shares_remaining, max_chunk_size)
            price = round(limit_price, 2)

            # ✅ Request delayed market data to satisfy IBKR precaution
            ib.reqMarketDataType(3)  # 3 = delayed
            ib.reqMktData(contract=contract, genericTickList="", snapshot=True)
            ib.sleep(1)

            # Place limit order
            order = LimitOrder(action, chunk_size, price)
            _ = ib.placeOrder(contract, order)
            print(
                f"🔄 Submitted {action} order for {chunk_size} shares of {symbol} @ ${price}"
            )

            ib.sleep(3)  # pacing buffer
            shares_remaining -= chunk_size

        print(f"✅ All orders for {symbol} submitted.")

    print("\n✅ All liquidation orders submitted.")


def get_account_status(ib):
    """Print current portfolio value and cash balance."""
    account_summary = ib.accountSummary()

    net_liquidation = next(
        (item.value for item in account_summary if item.tag == "NetLiquidation"), None
    )
    cash_balance = next(
        (item.value for item in account_summary if item.tag == "AvailableFunds"), None
    )

    print(f"\n🔸 Net Liquidation Value: ${net_liquidation}")
    print(f"🔸 Cash Balance: ${cash_balance}\n")

    # Portfolio positions
    positions = ib.positions()
    if positions:
        print("📄 Open Positions:")
        for pos in positions:
            print(f" - {pos.contract.symbol}: {pos.position} shares")
    else:
        print("✅ No open positions.")


if __name__ == "__main__":
    ib = IB()
    ib.connect(HOST, PORT, clientId=CLIENT_ID)

    # --- Call functions ---
    # get_account_status(ib)
    # reset_account(ib)
    get_account_status(ib)

    ib.disconnect()
