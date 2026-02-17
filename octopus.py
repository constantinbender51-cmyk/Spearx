#!/usr/bin/env python3
"""
Kraken Futures Copy Bot
1. Monitors BTC Perpetual position (Source).
2. Maintains equal Notional Value (USD) in PEPE and XRP Perps (Targets).
3. Logic:
   - If NO open order: Check if position deviates > 10%. If so, place Limit Order.
   - If OPEN order exists: Update (Edit) it to trail the current Mark Price.
4. Checks once per minute.
"""

import os
import sys
import time
import logging
import json
from typing import Dict, Any, List

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
LIMIT_OFFSET = 0.002        # 0.2% "Taker" offset (ensures fill). 
                            # Set to -0.001 for "Maker" (passive) behavior.

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
        symbol = symbol.upper()
        if symbol not in self.specs: return quantity
        lot_size = self.specs[symbol]["lotSize"]
        if lot_size == 0: return quantity
        return round(quantity / lot_size) * lot_size

    def _round_to_tick(self, symbol: str, price: float) -> float:
        symbol = symbol.upper()
        if symbol not in self.specs: return price
        tick_size = self.specs[symbol]["tickSize"]
        if tick_size == 0: return price
        return round(price / tick_size) * tick_size

    def get_data(self):
        """Fetches Positions, Prices, and Open Orders."""
        try:
            pos_resp = self.kf.get_open_positions()
            positions = {}
            if "openPositions" in pos_resp:
                for p in pos_resp["openPositions"]:
                    s = float(p["size"])
                    if p["side"] == "short": s = -s
                    positions[p["symbol"].upper()] = s
            
            tick_resp = self.kf.get_tickers()
            prices = {}
            if "tickers" in tick_resp:
                for t in tick_resp["tickers"]:
                    prices[t["symbol"].upper()] = float(t["markPrice"])

            ord_resp = self.kf.get_open_orders()
            open_orders = {} # Map Symbol -> Order Object
            if "openOrders" in ord_resp:
                for o in ord_resp["openOrders"]:
                    sym = o["symbol"].upper()
                    # We only care about one order per symbol for this simple bot
                    open_orders[sym] = o 

            return positions, prices, open_orders
        except Exception as e:
            logger.error(f"API Error fetching data: {e}")
            return {}, {}, {}

    def run(self):
        logger.info(f"--- Bot Started ---")
        logger.info(f"Source: {SOURCE_SYMBOL} | Limit Offset: {LIMIT_OFFSET*100}%")

        while True:
            try:
                positions, prices, open_orders = self.get_data()

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
                    existing_order = open_orders.get(target_sym)

                    # Calculate Targets
                    desired_value_usd = btc_value_usd 
                    raw_target_qty = desired_value_usd / target_price
                    target_qty = self._round_to_lot(target_sym, raw_target_qty)

                    # Determine Delta
                    diff = target_qty - current_qty
                    abs_diff = abs(diff)
                    final_size = self._round_to_lot(target_sym, abs_diff)
                    
                    # Determine Side
                    side = "buy" if diff > 0 else "sell"

                    # Calculate Safe Limit Price (Trailing)
                    # Buy = Mark * (1 + offset), Sell = Mark * (1 - offset)
                    if side == "buy":
                        raw_price = target_price * (1 + LIMIT_OFFSET)
                    else:
                        raw_price = target_price * (1 - LIMIT_OFFSET)
                    
                    limit_price = self._round_to_tick(target_sym, raw_price)

                    # --- Logic Branch ---
                    
                    # CASE A: Open Order Exists -> EDIT (Trail Price)
                    if existing_order:
                        order_id = existing_order.get("order_id") or existing_order.get("orderId")
                        
                        # Only edit if size or price is significantly different to avoid API spam
                        # But here we just send the edit to ensure tight trailing
                        logger.info(f"EDIT {target_sym}: Trailing to {limit_price} (Size: {final_size})")
                        
                        batch_instructions.append({
                            "order": "edit",
                            "orderId": order_id,
                            "symbol": target_sym.lower(),
                            "limitPrice": str(limit_price),
                            "size": str(final_size)
                        })

                    # CASE B: No Open Order -> Check Threshold -> SEND
                    else:
                        should_trade = False
                        # 0 -> Something
                        if current_qty == 0 and abs(target_qty) > 0:
                            should_trade = True
                        # Something -> 0
                        elif current_qty != 0 and target_qty == 0:
                            should_trade = True
                        # Something -> Something (Deviation)
                        elif current_qty != 0:
                            pct_deviation = abs_diff / abs(current_qty)
                            if pct_deviation > REBALANCE_THRESHOLD:
                                should_trade = True

                        if should_trade and final_size > 0:
                            logger.info(f"NEW ORDER {target_sym}: Delta {final_size} | {side.upper()} @ {limit_price}")
                            batch_instructions.append({
                                "order": "send",
                                "order_tag": "copy_bot",
                                "orderType": "lmt",
                                "symbol": target_sym.lower(),
                                "side": side,
                                "size": str(final_size),
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
                            logger.info(f"Instruction {i+1}: {status}")
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