#!/usr/bin/env python3
"""
Octopus Grid: Execution Engine for GA Grid Strategy
- Source: Connects to app.py endpoint (/api/parameters)
- Logic: Grid Entry (Nearest Lines) + Dynamic SL/TP
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
                
                # --- DEBUG: Print suppressed per user request ---
                # print(f"\nDEBUG_RAW_RESPONSE: {data}\n") 
                # --------------------------------------------

                if isinstance(data, dict):
                    valid_strategies = {}
                    for sym, params in data.items():
                        try:
                            if "line_prices" in params and "stop_percent" in params:
                                valid_strategies[sym] = params
                        except TypeError:
                            bot_log(f"Skipping invalid entry '{sym}': Data was not a dict (Got {type(params)})", level="warning")
                            continue
                            
                    return valid_strategies
                else:
                    bot_log("Invalid JSON structure from server (expected Dict).", level="warning")
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

    def initialize(self):
        bot_log("--- Initializing Octopus Grid Bot (Multi-Asset 1m Cycle) ---")
        
        self._fetch_instrument_specs()
        
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                bot_log(f"API Error: {acc}", level="error")
                sys.exit(1)
            else:
                bot_log("API Connection Successful.")

            # --- 1. Cancel All Orders on Startup ---
            bot_log("Startup: Canceling all open orders for mapped assets...")
            unique_symbols = set(SYMBOL_MAP.values())
            for sym in unique_symbols:
                try:
                    # API usually expects lowercase symbols for order management
                    self.kf.cancel_all_orders({"symbol": sym.lower()})
                    bot_log(f"Cancelled orders for {sym}")
                except Exception as e:
                    bot_log(f"Failed to cancel {sym}: {e}", level="warning")
                    
        except Exception as e:
            bot_log(f"Startup Failed: {e}", level="error")
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
                        bot_log(f"Loaded Specs for {sym}: {self.instrument_specs[sym]}")
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
            bot_log(f"[{kf_symbol_upper}] Ticker Fetch Error: {e}", level="error")
            return 0.0

    def run(self):
        bot_log("Bot started. Syncing to 1m intervals...")
        while True:
            now = datetime.now(timezone.utc)
            # Trigger every minute at :05 seconds
            if now.second >= 5 and now.second < 10:
                bot_log(f">>> TRIGGER: {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                time.sleep(50) 
            time.sleep(1)

    def _process_cycle(self):
        # 1. Fetch ALL Strategy Parameters
        all_params = self.fetcher.fetch_all_params()
        if not all_params:
            bot_log("No params received. Skipping cycle.", level="warning")
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

        # 3. Determine Execution Size per Asset (TESTING LIMIT APPLIED HERE)
        limited_keys = list(all_params.keys())[:TEST_ASSET_LIMIT]
        limited_params = {k: all_params[k] for k in limited_keys}
        
        active_assets_count = len(limited_params)
        
        bot_log(f"TEST MODE: Limiting active assets to {active_assets_count}: {limited_keys}")
        
        if active_assets_count == 0: return
        
        # 4. Execute Logic for Each Asset
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in limited_params.items():
                kraken_symbol = SYMBOL_MAP.get(app_symbol)
                
                if not kraken_symbol:
                    bot_log(f"Skipping {app_symbol}: No Kraken mapping found.", level="warning")
                    continue
                
                executor.submit(
                    self._execute_grid_logic, 
                    kraken_symbol, 
                    equity, 
                    params, 
                    active_assets_count
                )

    def _execute_grid_logic(self, symbol_str: str, equity: float, params: Dict, asset_count: int):
        symbol_upper = symbol_str.upper()
        symbol_lower = symbol_str.lower()
        
        grid_lines = np.sort(np.array(params["line_prices"]))
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]

        specs = self.instrument_specs.get(symbol_upper)
        if not specs: 
            bot_log(f"No specs for {symbol_upper} - skipping.", level="error")
            return

        pos_size, entry_price = self._get_position_details(symbol_upper)
        current_price = self._get_mark_price(symbol_upper)
        
        if current_price == 0: return

        is_flat = abs(pos_size) < specs["sizeStep"]
        
        bot_log(f"[{symbol_upper}] Price: {current_price} | Pos: {pos_size} @ {entry_price}")

        if is_flat:
            # --- FLAT: Place Grid Entry Orders ---
            idx = np.searchsorted(grid_lines, current_price)
            line_below = grid_lines[idx-1] if idx > 0 else None
            line_above = grid_lines[idx] if idx < len(grid_lines) else None
            
            # Split Equity Logic
            safe_equity = equity * 0.95
            allocation_per_asset = safe_equity / max(1, asset_count)
            unit_usd = (allocation_per_asset * LEVERAGE) * 0.20 
            
            qty = unit_usd / current_price
            qty = self._round_to_step(qty, specs["sizeStep"])

            if qty < specs["sizeStep"]:
                bot_log(f"[{symbol_upper}] Qty too small ({qty}).", level="warning")
                return

            try: self.kf.cancel_all_orders({"symbol": symbol_lower})
            except: pass
            
            if line_below:
                price = self._round_to_step(line_below, specs["tickSize"])
                if price < current_price:
                    bot_log(f"[{symbol_upper}] Placing BUY LMT @ {price} (Qty: {qty})")
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                        "size": qty, "limitPrice": price
                    })
            
            if line_above:
                price = self._round_to_step(line_above, specs["tickSize"])
                if price > current_price:
                    bot_log(f"[{symbol_upper}] Placing SELL LMT @ {price} (Qty: {qty})")
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                        "size": qty, "limitPrice": price
                    })

        else:
            # --- IN POSITION: Manage Exits ---
            try:
                open_orders = self.kf.get_open_orders().get("openOrders", [])
                for o in open_orders:
                    if o["symbol"].lower() == symbol_lower and o["orderType"] == "lmt":
                        # We cancel basic limits, but keep Take Profits (if they are type 'lmt' with reduceOnly)
                        if not o.get("reduceOnly", False):
                            self.kf.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})
            except Exception as e:
                bot_log(f"[{symbol_upper}] Cleanup error: {e}", level="warning")

            has_sl = False
            has_tp = False
            
            try:
                open_orders = self.kf.get_open_orders().get("openOrders", [])
                for o in open_orders:
                    if o["symbol"].lower() == symbol_lower:
                        # Check for STP
                        if o["orderType"] == "stp": has_sl = True
                        # Check for TP (using LMT + reduceOnly)
                        if o["orderType"] == "lmt" and o.get("reduceOnly", False): has_tp = True
            except: pass

            if not has_sl or not has_tp:
                self._place_bracket_orders(symbol_lower, pos_size, entry_price, stop_pct, profit_pct, specs["tickSize"])

    def _place_bracket_orders(self, symbol: str, position_size: float, entry_price: float, 
                              sl_pct: float, tp_pct: float, tick_size: float):
        is_long = position_size > 0
        side = "sell" if is_long else "buy"
        abs_size = abs(position_size)

        if is_long:
            sl_price = entry_price * (1 - sl_pct)
            tp_price = entry_price * (1 + tp_pct)
        else:
            sl_price = entry_price * (1 + sl_pct)
            tp_price = entry_price * (1 - tp_pct)

        sl_price = self._round_to_step(sl_price, tick_size)
        tp_price = self._round_to_step(tp_price, tick_size)

        bot_log(f"[{symbol.upper()}] Adding Brackets | Entry: {entry_price} | SL: {sl_price} | TP: {tp_price}")

        # STOP LOSS: 'stp' type with limitPrice + triggerSignal="mark"
        try:
            sl_resp = self.kf.send_order({
                "orderType": "stp", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": sl_price, 
                "limitPrice": sl_price, 
                "triggerSignal": "mark",  # Explicitly use Mark price to avoid wicks
                "reduceOnly": True
            })
            
            # --- Status Inspection ---
            status = "unknown"
            if "sendStatus" in sl_resp and "orderEvents" in sl_resp["sendStatus"]:
                events = sl_resp["sendStatus"]["orderEvents"]
                if events:
                    status = events[0].get("order", {}).get("status", "unknown")
            
            bot_log(f"SL Response [{symbol.upper()}]: Status={status} | {sl_resp}")

        except Exception as e:
            bot_log(f"[{symbol.upper()}] SL Failed: {e}", level="error")

        # TAKE PROFIT: 'lmt' type with limitPrice
        try:
            tp_resp = self.kf.send_order({
                "orderType": "lmt", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "limitPrice": tp_price, 
                "reduceOnly": True
            })
            
            status = "unknown"
            if "sendStatus" in tp_resp and "orderEvents" in tp_resp["sendStatus"]:
                events = tp_resp["sendStatus"]["orderEvents"]
                if events:
                    status = events[0].get("order", {}).get("status", "unknown")
            
            bot_log(f"TP Response [{symbol.upper()}]: Status={status} | {tp_resp}")

        except Exception as e:
            bot_log(f"[{symbol.upper()}] TP Failed: {e}", level="error")

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
