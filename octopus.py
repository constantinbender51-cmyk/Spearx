#!/usr/bin/env python3
"""
Octopus Grid: Execution Engine for GA Grid Strategy
- Source: Connects to app.py endpoint (/api/parameters)
- Logic: Dual-Order State Machine (Entry Bracket -> Exit Bracket)
- Schedule: Every 1 minute
- Assets: Multi-Asset Support (Maps app.py symbols to Kraken Futures)
"""

import os
import sys
import time
import logging
import requests
import threading
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, Any, List

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

# API Keys
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Global Settings
MAX_WORKERS = 16
LEVERAGE = 10
TEST_ASSET_LIMIT = 4  # Limit execution to 4 assets for testing

# Strategy Endpoint (The app.py server)
STRATEGY_URL = "https://machine-learning.up.railway.app/api/parameters"

# Asset Mapping: App Symbol -> Kraken Futures Symbol
SYMBOL_MAP = {
    "BTC": "PF_XBTUSD",
    "ETH": "PF_ETHUSD",
    "XRP": "PF_XRPUSD",
    "SOL": "PF_SOLUSD",
    "DOGE": "PF_DOGEUSD",
    "ADA": "PF_ADAUSD",
    "BCH": "PF_BCHUSD",
    "LINK": "PF_LINKUSD",
    "XLM": "PF_XLMUSD",
    "SUI": "PF_SUIUSD",
    "AVAX": "PF_AVAXUSD",
    "LTC": "PF_LTCUSD",
    "HBAR": "PF_HBARUSD",
    "SHIB": "PF_SHIBUSD",
    "TON": "PF_TONUSD",
}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("grid_octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OctopusGrid")
LOG_LOCK = threading.Lock()

def bot_log(msg, level="info"):
    with LOG_LOCK:
        if level == "info": logger.info(msg)
        elif level == "warning": logger.warning(msg)
        elif level == "error": logger.error(msg)

# --- Strategy Fetcher ---

class GridParamFetcher:
    def fetch_all_params(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches ALL parameters from the GA server.
        """
        bot_log(f"Fetching grid params from {STRATEGY_URL}...")
        try:
            resp = requests.get(STRATEGY_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    valid_strategies = {}
                    for sym, params in data.items():
                        try:
                            if "line_prices" in params and "stop_percent" in params:
                                valid_strategies[sym] = params
                        except TypeError:
                            continue
                    return valid_strategies
                else:
                    bot_log("Invalid JSON structure from server.", level="warning")
            else:
                bot_log(f"HTTP Error {resp.status_code}", level="warning")
        except Exception as e:
            bot_log(f"Param Fetch Failed: {e}", level="error")
        return {}

# --- Main Engine ---

class OctopusGridBot:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = GridParamFetcher()
        self.instrument_specs = {}
        self.is_initialized = False

    def initialize(self):
        bot_log("--- Initializing Octopus Grid Bot (Strict 2-Order Logic) ---")
        self._fetch_instrument_specs()
        
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                bot_log(f"API Error: {acc}", level="error")
                sys.exit(1)
            else:
                bot_log("API Connection Successful.")
        except Exception as e:
            bot_log(f"API Connection Failed: {e}", level="error")
            sys.exit(1)

    def _fetch_instrument_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                target_kraken_symbols = set(SYMBOL_MAP.values())
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper()
                    if sym in target_kraken_symbols:
                        precision = inst.get("contractValueTradePrecision", 3)
                        self.instrument_specs[sym] = {
                            "sizeStep": 10 ** (-int(precision)) if precision is not None else 1.0,
                            "tickSize": float(inst.get("tickSize", 0.5))
                        }
        except Exception as e:
            bot_log(f"Error fetching specs: {e}", level="error")

    def _round_to_step(self, value: float, step: float) -> float:
        if step == 0: return value
        rounded = round(value / step) * step
        return float(f"{rounded:.10g}")

    def _get_position_details(self, kf_symbol_upper: str) -> Tuple[float, float]:
        """Returns (size, avgEntryPrice)"""
        try:
            open_pos = self.kf.get_open_positions()
            if "openPositions" in open_pos:
                for p in open_pos["openPositions"]:
                    if p["symbol"].upper() == kf_symbol_upper:
                        size = float(p["size"])
                        if p["side"] == "short": size = -size
                        entry = float(p["price"])
                        return size, entry
            return 0.0, 0.0
        except Exception as e:
            bot_log(f"[{kf_symbol_upper}] Position Fetch Error: {e}", level="error")
            return 0.0, 0.0

    def _get_mark_price(self, kf_symbol_upper: str) -> float:
        try:
            tickers = self.kf.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == kf_symbol_upper:
                    return float(t["markPrice"])
            return 0.0
        except Exception as e:
            return 0.0

    def _initial_reset(self):
        """
        Req 1: Initially close all orders and positions.
        """
        bot_log(">>> PERFORMING INITIAL RESET (Closing All) <<<")
        
        # 1. Cancel All Orders
        try:
            self.kf.cancel_all_orders()
            bot_log("All open orders cancelled.")
        except Exception as e:
            bot_log(f"Reset Cancel Error: {e}", level="error")

        # 2. Close All Positions
        try:
            positions = self.kf.get_open_positions().get("openPositions", [])
            for p in positions:
                sym = p["symbol"]
                size = float(p["size"])
                side = p["side"] # 'long' or 'short'
                
                # To close, we do opposite side
                close_side = "sell" if side == "long" else "buy"
                
                bot_log(f"Closing position: {sym} {side} {size}")
                self.kf.send_order({
                    "orderType": "mkt",
                    "symbol": sym,
                    "side": close_side,
                    "size": size,
                    "reduceOnly": True
                })
            time.sleep(2) # Allow fills
        except Exception as e:
            bot_log(f"Reset Position Close Error: {e}", level="error")
            
        self.is_initialized = True
        bot_log(">>> INITIAL RESET COMPLETE <<<")

    def run(self):
        bot_log("Bot started. Syncing to 1m intervals...")
        while True:
            now = datetime.now(timezone.utc)
            if now.second >= 5 and now.second < 10:
                bot_log(f">>> TRIGGER: {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                time.sleep(50) 
            time.sleep(1)

    def _process_cycle(self):
        # Perform Hard Reset on first run only
        if not self.is_initialized:
            self._initial_reset()

        # 1. Fetch ALL Strategy Parameters
        all_params = self.fetcher.fetch_all_params()
        if not all_params:
            return
        
        # 2. Account Health Check
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            else:
                first_acc = list(acc.get("accounts", {}).values())[0]
                equity = float(first_acc.get("marginEquity", 0))
                
            if equity <= 0:
                bot_log("Equity <= 0. Aborting.", level="error")
                return
        except Exception as e:
            bot_log(f"Account fetch failed: {e}", level="error")
            return

        # 3. Apply Test Limits
        limited_keys = list(all_params.keys())[:TEST_ASSET_LIMIT]
        limited_params = {k: all_params[k] for k in limited_keys}
        active_assets_count = len(limited_params)
        
        if active_assets_count == 0: return
        
        # 4. Execute Logic
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in limited_params.items():
                kraken_symbol = SYMBOL_MAP.get(app_symbol)
                if not kraken_symbol: continue
                
                executor.submit(
                    self._execute_state_machine, 
                    kraken_symbol, 
                    equity, 
                    params, 
                    active_assets_count
                )

    def _execute_state_machine(self, symbol_str: str, equity: float, params: Dict, asset_count: int):
        symbol_upper = symbol_str.upper()
        symbol_lower = symbol_str.lower()
        
        grid_lines = np.sort(np.array(params["line_prices"]))
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]

        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        # 1. Get Real-Time State
        pos_size, entry_price = self._get_position_details(symbol_upper)
        current_price = self._get_mark_price(symbol_upper)
        
        if current_price == 0: return

        # Fetch existing orders for this asset
        existing_orders = []
        try:
            all_open = self.kf.get_open_orders().get("openOrders", [])
            existing_orders = [o for o in all_open if o["symbol"] == symbol_lower]
        except:
            return

        is_flat = abs(pos_size) < specs["sizeStep"]
        
        bot_log(f"[{symbol_upper}] Price:{current_price} | Pos:{pos_size} | Orders:{len(existing_orders)}")

        # --- STATE MACHINE LOGIC ---

        if is_flat:
            # === STATE: FLAT ===
            # Req 4 & 2: If we are flat, we need to be ready to enter.
            # We must ensure NO "Stop" or "Profit" orders exist (leftovers from closed trades).
            # We must ensure EXACTLY 2 Entry orders exist (Long Limit + Short Limit).

            # 1. Clean up "Pollution" (Leftover SL/TP from previous trade)
            dirty_orders = [o for o in existing_orders if o["orderType"] in ["stp", "take_profit"]]
            if dirty_orders:
                bot_log(f"[{symbol_upper}] FLAT: Cleaning up {len(dirty_orders)} stale exit orders.")
                self.kf.cancel_all_orders({"symbol": symbol_lower})
                existing_orders = [] # Reset local list as we wiped them

            # 2. Check for Valid Entry Orders
            # We expect exactly 1 Buy LMT and 1 Sell LMT
            buy_orders = [o for o in existing_orders if o["side"] == "buy" and o["orderType"] == "lmt"]
            sell_orders = [o for o in existing_orders if o["side"] == "sell" and o["orderType"] == "lmt"]

            # If we don't have exactly 1 of each, we reset and place new ones.
            if len(buy_orders) != 1 or len(sell_orders) != 1:
                bot_log(f"[{symbol_upper}] FLAT: Setting up fresh Entry pair.")
                
                # Cancel partials if any
                if existing_orders:
                    self.kf.cancel_all_orders({"symbol": symbol_lower})
                
                # Calc Quantities
                safe_equity = equity * 0.95
                allocation_per_asset = safe_equity / max(1, asset_count)
                unit_usd = (allocation_per_asset * LEVERAGE) * 0.20 
                qty = unit_usd / current_price
                qty = self._round_to_step(qty, specs["sizeStep"])

                if qty < specs["sizeStep"]:
                    bot_log(f"[{symbol_upper}] Qty too small ({qty})")
                    return

                # Find Lines
                idx = np.searchsorted(grid_lines, current_price)
                line_below = grid_lines[idx-1] if idx > 0 else None
                line_above = grid_lines[idx] if idx < len(grid_lines) else None

                # Place Long
                if line_below:
                    p = self._round_to_step(line_below, specs["tickSize"])
                    if p < current_price:
                        self.kf.send_order({
                            "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                            "size": qty, "limitPrice": p
                        })
                
                # Place Short
                if line_above:
                    p = self._round_to_step(line_above, specs["tickSize"])
                    if p > current_price:
                        self.kf.send_order({
                            "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                            "size": qty, "limitPrice": p
                        })
            else:
                # We have our 2 orders. We are waiting.
                pass

        else:
            # === STATE: IN POSITION ===
            # Req 3: "Cancel the other order and place a stop and a profit"
            
            # 1. Clean up "Pollution" (Entry Limit Orders that shouldn't be here)
            # If we are in a position, any LMT order is the "other" order that didn't trigger.
            entry_leftovers = [o for o in existing_orders if o["orderType"] == "lmt"]
            if entry_leftovers:
                bot_log(f"[{symbol_upper}] IN POS: Cancelling {len(entry_leftovers)} unused Entry orders.")
                for o in entry_leftovers:
                    self.kf.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})
            
            # 2. Check for Bracket Orders
            # We need 1 Stop Loss and 1 Take Profit
            sl_orders = [o for o in existing_orders if o["orderType"] == "stp"]
            tp_orders = [o for o in existing_orders if o["orderType"] == "take_profit"]

            if len(sl_orders) == 0 or len(tp_orders) == 0:
                bot_log(f"[{symbol_upper}] IN POS: Placing missing Brackets.")
                
                # Ideally we check what's missing, but for robustness:
                # If either is missing, we clear existing brackets and replace BOTH to ensure consistency.
                if sl_orders or tp_orders:
                    # Cancel existing partial brackets
                    for o in sl_orders + tp_orders:
                        self.kf.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})

                # Calculate Prices
                is_long = pos_size > 0
                exit_side = "sell" if is_long else "buy"
                abs_size = abs(pos_size)
                
                if is_long:
                    sl_price = entry_price * (1 - stop_pct)
                    tp_price = entry_price * (1 + profit_pct)
                else:
                    sl_price = entry_price * (1 + stop_pct)
                    tp_price = entry_price * (1 - profit_pct)
                
                sl_price = self._round_to_step(sl_price, specs["tickSize"])
                tp_price = self._round_to_step(tp_price, specs["tickSize"])

                # Place Stop
                self.kf.send_order({
                    "orderType": "stp", "symbol": symbol_lower, "side": exit_side,
                    "size": abs_size, "stopPrice": sl_price, "reduceOnly": True
                })
                
                # Place Profit
                self.kf.send_order({
                    "orderType": "take_profit", "symbol": symbol_lower, "side": exit_side,
                    "size": abs_size, "stopPrice": tp_price, "reduceOnly": True
                })

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
