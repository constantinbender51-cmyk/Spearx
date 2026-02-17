#!/usr/bin/env python3
"""
Kraken Futures Copy Bot
1. Monitors BTC Perpetual position (Source).
2. Maintains equal Notional Value (USD) in PEPE and XRP Perps (Targets).
3. Logic:
   - Filters Open Orders by 'order_tag' to find its own orders.
   - If OWN order exists: Edits it to trail the Mark Price.
   - If NO OWN order: Checks 10% deviation. If triggered, places new Limit Order.
4. Robust 'invalidSize' fix: Uses 'contractValueTradePrecision' (positive/negative)
   to correctly format order sizes for all instruments.
"""

import os
import sys
import time
import logging
import json
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

# Symbols
SOURCE_SYMBOL = "PF_XBTUSD"
TARGET_SYMBOLS = ["PF_PEPEUSD", "PF_XRPUSD"]

# Settings
REBALANCE_THRESHOLD = 0.10  # 10% deviation trigger
LIMIT_OFFSET = 0.002        # 0.2% Taker offset for Limit Orders

# Identity Tag (Critical for finding our own orders)
BOT_TAG = "copy_bot"

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
        self.specs = {} 

    def initialize(self):
        logger.info("Fetching instrument specifications...")
        try:
            resp = self.kf.get_instruments()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper()
                    
                    # CRITICAL FIX: Use contractValueTradePrecision directly from API
                    precision = int(inst.get("contractValueTradePrecision", 0))
                        
                    self.specs[sym] = {
                        "lotSize": float(inst.get("lotSize", 1.0)),
                        "tickSize": float(inst.get("tickSize", 0.0001)),
                        "precision": precision # Store the positive, zero, or negative value
                    }
                logger.info(f"Loaded specs for {len(self.specs)} instruments.")
            else:
                logger.error("Failed to load instruments.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Initialization Error: {e}")
            sys.exit(1)

    def _round_to_lot(self, symbol: str, quantity: float) -> float:
        """Rounds a quantity to the nearest valid lot size."""
        symbol = symbol.upper()
        if symbol not in self.specs: return quantity
        lot_size = self.specs[symbol]["lotSize"]
        if lot_size == 0: return quantity
        return round(quantity / lot_size) * lot_size

    def _round_to_tick(self, symbol: str, price: float) -> float:
        """Rounds a price to the nearest valid tick size."""
        symbol = symbol.upper()
        if symbol not in self.specs: return price
        tick_size = self.specs[symbol]["tickSize"]
        if tick_size == 0: return price
        return round(price / tick_size) * tick_size

    def _format_size(self, symbol: str, size: float) -> str:
        """Formats size to a string based on contractValueTradePrecision."""
        symbol = symbol.upper()
        if symbol not in self.specs:
            return str(size)

        precision = self.specs[symbol]["precision"]

        if precision >= 0:
            # Positive precision: format to N decimal places
            return f"{size:.{precision}f}"
        else:
            # Negative precision: round to nearest 10, 100, 1000 etc.
            # e.g., precision = -3 means round to nearest 1000
            divisor = 10 ** abs(precision)
            rounded_size = round(size / divisor) * divisor
            # The result must be an integer, so format as such to avoid ".0"
            return str(int(rounded_size))

    def get_data(self):
        """Fetches Positions, Prices, and OUR Open Orders."""
        try:
            # 1. Positions
            pos_resp = self.kf.get_open_positions()
            positions = {}
            if "openPositions" in pos_resp:
                for p in pos_resp["openPositions"]:
                    s = float(p["size"])
                    if p["side"] == "short": s = -s
                    positions[p["symbol"].upper()] = s
            
            # 2. Prices
            tick_resp = self.kf.get_tickers()
            prices = {}
            if "tickers" in tick_resp:
                for t in tick_resp["tickers"]:
                    prices[t["symbol"].upper()] = float(t["markPrice"])

            # 3. Open Orders (Filtered by TAG)
            ord_resp = self.kf.get_open_orders()
            my_orders = {} 
            if "openOrders" in ord_resp:
                for o in ord_resp["openOrders"]:
                    tag = o.get("order_tag", "") or o.get("orderTag", "")
                    cli_id = o.get("cliOrdId", "") or o.get("clientOrderId", "")
                    
                    if tag == BOT_TAG or cli_id.startswith("cb_"):
                        sym = o["symbol"].upper()
                        my_orders[sym] = o 

            return positions, prices, my_orders
        except Exception as e:
            logger.error(f"API Error fetching data: {e}")
            return {}, {}, {}

    def run(self):
        logger.info(f"--- Bot Started ---")
        logger.info(f"Source: {SOURCE_SYMBOL} | Tag: {BOT_TAG}")

        while True:
            try:
                positions, prices, my_orders = self.get_data()

                if SOURCE_SYMBOL not in prices:
                    logger.warning(f"Price for {SOURCE_SYMBOL} not found. Skipping.")
                    time.sleep(10)
                    continue

                # 1. Calculate Source Value
                btc_size = positions.get(SOURCE_SYMBOL, 0.0)
                btc_price = prices[SOURCE_SYMBOL]
                btc_value_usd = btc_size * btc_price

                logger.info(f"Source {SOURCE_SYMBOL}: Size {btc_size:.4f} | Value ${btc_value_usd:.2f}")

                batch_instructions = []

                # 2. Process Targets
                for target_sym in TARGET_SYMBOLS:
                    if target_sym not in prices:
                        continue

                    target_price = prices[target_sym]
                    current_qty = positions.get(target_sym, 0.0)
                    existing_order = my_orders.get(target_sym)

                    # Calculate Targets
                    desired_value_usd = btc_value_usd 
                    raw_target_qty = desired_value_usd / target_price
                    target_qty = self._round_to_lot(target_sym, raw_target_qty)

                    # Determine Delta
                    diff = target_qty - current_qty
                    abs_diff = abs(diff)
                    final_size_float = self._round_to_lot(target_sym, abs_diff)
                    
                    # Format size string using dynamic precision (Fixes invalidSize)
                    size_str = self._format_size(target_sym, final_size_float)

                    # Determine Side
                    side = "buy" if diff > 0 else "sell"

                    # Calculate Safe Limit Price
                    if side == "buy":
                        raw_price = target_price * (1 + LIMIT_OFFSET)
                    else:
                        raw_price = target_price * (1 - LIMIT_OFFSET)
                    
                    limit_price = self._round_to_tick(target_sym, raw_price)

                    # --- Logic Branch ---
                    
                    # CASE A: We have an existing order -> EDIT IT
                    if existing_order:
                        order_id = existing_order.get("order_id") or existing_order.get("orderId")
                        logger.info(f"EDIT {target_sym} (ID: {order_id}): Trailing to {limit_price} | Size: {size_str}")
                        
                        batch_instructions.append({
                            "order": "edit",
                            "orderId": order_id,
                            "symbol": target_sym.lower(),
                            "limitPrice": str(limit_price),
                            "size": size_str 
                        })

                    # CASE B: No existing order -> CHECK DEVIATION -> SEND NEW
                    else:
                        should_trade = False
                        if current_qty == 0 and abs(target_qty) > 0:
                            should_trade = True
                        elif current_qty != 0 and target_qty == 0:
                            should_trade = True
                        elif current_qty != 0:
                            pct_deviation = abs_diff / abs(current_qty)
                            if pct_deviation > REBALANCE_THRESHOLD:
                                should_trade = True

                        if should_trade and final_size_float > 0:
                            logger.info(f"NEW ORDER {target_sym}: Delta {size_str} | {side.upper()} @ {limit_price}")
                            batch_instructions.append({
                                "order": "send",
                                "order_tag": BOT_TAG,
                                "orderType": "lmt",
                                "symbol": target_sym.lower(),
                                "side": side,
                                "size": size_str,
                                "limitPrice": str(limit_price),
                                "cliOrdId": f"cb_{int(time.time()*1000)}_{target_sym[-3:]}"
                            })

                # 3. Execute Batch
                if batch_instructions:
                    logger.info(f"Sending {len(batch_instructions)} instructions...")
                    wrapper = {"batchOrder": batch_instructions}
                    payload = {"json": json.dumps(wrapper)}
                    
                    resp = self.kf.batch_order(payload)
                    
                    if "batchStatus" in resp:
                        for i, res in enumerate(resp["batchStatus"]):
                            status = res.get("status")
                            err = res.get("error", "")
                            oid = res.get("orderId") or res.get("order_id") or "N/A"
                            if status in ["placed", "edited"]:
                                logger.info(f"Instruction {i+1} SUCCESS: {status} (ID: {oid})")
                            else:
                                logger.error(f"Instruction {i+1} FAILED: {status} - {err}")
                    else:
                        logger.error(f"Batch failed: {resp}")
                else:
                    logger.info("No updates needed.")

            except Exception as e:
                logger.error(f"Loop Exception: {e}")

            time.sleep(60)

if __name__ == "__main__":
    bot = CopyBot()
    bot.initialize()
    bot.run()