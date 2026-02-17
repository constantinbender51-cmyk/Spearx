#!/usr/bin/env python3
"""
Kraken Futures Copy Bot
1. Monitors BTC Perpetual position (Source).
2. Maintains equal Notional Value (USD) in PEPE and XRP Perps (Targets).
3. Rebalances only if the required position size deviates > 10% from actual.
4. Checks once per minute.
"""

import os
import sys
import time
import logging
import json  # <--- Added import
from typing import Dict, Any

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found.")
    sys.exit(1)

# --- Configuration ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KEY = os.getenv("KRAKEN_FUTURES_KEY")
SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Symbols (Must be correct Kraken Futures identifiers)
SOURCE_SYMBOL = "PF_XBTUSD"
TARGET_SYMBOLS = ["PF_PEPEUSD", "PF_XRPUSD"]

# Threshold (10%)
REBALANCE_THRESHOLD = 0.10

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("CopyBot")

class CopyBot:
    def __init__(self):
        if not KEY or not SECRET:
            logger.error("Missing API Key/Secret in environment variables.")
            sys.exit(1)
            
        self.kf = KrakenFuturesApi(KEY, SECRET)
        self.specs = {} # Stores lot sizes and precisions

    def initialize(self):
        """Fetch instrument specs to ensure we send valid order sizes."""
        logger.info("Fetching instrument specifications...")
        try:
            resp = self.kf.get_instruments()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper()
                    self.specs[sym] = {
                        "lotSize": float(inst.get("lotSize", 1.0)),
                        "tickSize": float(inst.get("tickSize", 0.0001))
                    }
                logger.info(f"Loaded specs for {len(self.specs)} instruments.")
            else:
                logger.error("Failed to load instruments.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Initialization Error: {e}")
            sys.exit(1)

    def _round_to_lot(self, symbol: str, quantity: float) -> float:
        """Rounds a quantity to the nearest valid lot size for the symbol."""
        symbol = symbol.upper()
        if symbol not in self.specs:
            return quantity
        
        lot_size = self.specs[symbol]["lotSize"]
        # Round to nearest lot
        steps = round(quantity / lot_size)
        return steps * lot_size

    def get_market_data(self):
        """Fetches current positions and market prices."""
        try:
            # 1. Get Positions
            pos_resp = self.kf.get_open_positions()
            positions = {}
            if "openPositions" in pos_resp:
                for p in pos_resp["openPositions"]:
                    s = float(p["size"])
                    if p["side"] == "short":
                        s = -s
                    positions[p["symbol"].upper()] = s
            
            # 2. Get Prices
            tick_resp = self.kf.get_tickers()
            prices = {}
            if "tickers" in tick_resp:
                for t in tick_resp["tickers"]:
                    prices[t["symbol"].upper()] = float(t["markPrice"])
            
            return positions, prices
        except Exception as e:
            logger.error(f"API Error fetching data: {e}")
            return {}, {}

    def run(self):
        logger.info(f"--- Bot Started ---")
        logger.info(f"Source: {SOURCE_SYMBOL} | Targets: {TARGET_SYMBOLS}")
        logger.info(f"Update Interval: 60s | Threshold: {REBALANCE_THRESHOLD*100}%")

        while True:
            try:
                positions, prices = self.get_market_data()

                # --- 1. Analyze Source (BTC) ---
                if SOURCE_SYMBOL not in prices:
                    logger.warning(f"Price for {SOURCE_SYMBOL} not found. Skipping cycle.")
                    time.sleep(10)
                    continue

                btc_size = positions.get(SOURCE_SYMBOL, 0.0)
                btc_price = prices[SOURCE_SYMBOL]
                
                # Calculate Notional Value (Size * Price)
                btc_value_usd = btc_size * btc_price

                logger.info(f"Source {SOURCE_SYMBOL}: Size {btc_size:.4f} | Value ${btc_value_usd:.2f}")

                # --- 2. Check Targets ---
                orders_to_send = []

                for target_sym in TARGET_SYMBOLS:
                    if target_sym not in prices:
                        logger.warning(f"Price for {target_sym} not found. Skipping.")
                        continue

                    target_price = prices[target_sym]
                    current_qty = positions.get(target_sym, 0.0)

                    # Calculate Desired Quantity (Target Value == BTC Value)
                    desired_value_usd = btc_value_usd 
                    raw_target_qty = desired_value_usd / target_price
                    
                    # Rounding
                    target_qty = self._round_to_lot(target_sym, raw_target_qty)

                    # --- 3. Deviation Logic ---
                    should_trade = False
                    
                    # Scenario A: We have 0, but want some (BTC position exists)
                    if current_qty == 0 and abs(target_qty) > 0:
                        should_trade = True
                    
                    # Scenario B: We have some, but want 0 (BTC position closed)
                    elif current_qty != 0 and target_qty == 0:
                        should_trade = True
                        
                    # Scenario C: We have some, compare deviation
                    elif current_qty != 0:
                        diff = target_qty - current_qty
                        pct_deviation = abs(diff) / abs(current_qty)
                        if pct_deviation > REBALANCE_THRESHOLD:
                            should_trade = True
                        else:
                            pass # Deviation too small

                    # --- 4. Prepare Order ---
                    if should_trade:
                        # Calculate trade size (delta)
                        trade_size = target_qty - current_qty
                        side = "buy" if trade_size > 0 else "sell"
                        
                        abs_size = abs(trade_size)
                        # Re-round the delta to be safe (ensure it meets lot size)
                        abs_size = self._round_to_lot(target_sym, abs_size)

                        if abs_size > 0:
                            logger.info(f"REBALANCE {target_sym}: Curr {current_qty} -> Targ {target_qty} | Delta: {side.upper()} {abs_size}")
                            
                            orders_to_send.append({
                                "orderType": "mkt",
                                "symbol": target_sym.lower(), 
                                "side": side,
                                "size": abs_size
                            })
                    else:
                        pass

                # --- 5. Execute Batch ---
                if orders_to_send:
                    logger.info(f"Sending {len(orders_to_send)} orders...")
                    
                    # --- FIX: Serialize list to JSON string for API ---
                    batch_payload = {"batchOrder": json.dumps(orders_to_send)}
                    
                    resp = self.kf.batch_order(batch_payload)
                    
                    if "batchStatus" in resp:
                         # Log results
                         statuses = resp.get("batchStatus", [])
                         for i, res in enumerate(statuses):
                             if "order_id" in res:
                                 logger.info(f"Order {i+1} OK: {res['order_id']}")
                             else:
                                 logger.error(f"Order {i+1} Failed: {res}")
                    else:
                        logger.error(f"Batch failed: {resp}")
                else:
                    logger.info("No rebalance needed.")

            except Exception as e:
                logger.error(f"Loop Exception: {e}")

            # Sleep 60 seconds
            time.sleep(60)

if __name__ == "__main__":
    bot = CopyBot()
    bot.initialize()
    bot.run()