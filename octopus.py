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
import random
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
MAX_WORKERS = 2 
LEVERAGE = 10
TEST_ASSET_LIMIT = 15

# Strategy Endpoint (The app.py server)
STRATEGY_URL = "https://spear22.up.railway.app/api/parameters"

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
                            bot_log(f"Skipping invalid entry '{sym}': Data was not a dict", level="warning")
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

            # --- 1. Cancel All Orders on Startup (Throttled) ---
            bot_log("Startup: Canceling all open orders for mapped assets...")
            unique_symbols = set(SYMBOL_MAP.values())
            
            for i, sym in enumerate(unique_symbols):
                try:
                    self.kf.cancel_all_orders({"symbol": sym.lower()})
                    bot_log(f"[{i+1}/{len(unique_symbols)}] Cancelled orders for {sym}")
                    time.sleep(1.2) 
                except Exception as e:
                    bot_log(f"Failed to cancel {sym}: {e}", level="warning")
                    time.sleep(2.0) 
                    
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
            if now.second >= 5 and now.second < 10:
                bot_log(f">>> TRIGGER: {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                time.sleep(50) 
            time.sleep(1)

    def _process_cycle(self):
        all_params = self.fetcher.fetch_all_params()
        if not all_params:
            bot_log("No params received. Skipping cycle.", level="warning")
            return
        
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

        limited_keys = list(all_params.keys())[:TEST_ASSET_LIMIT]
        limited_params = {k: all_params[k] for k in limited_keys}
        
        active_assets_count = len(limited_params)
        bot_log(f"Cycle: Processing {active_assets_count} assets with {MAX_WORKERS} workers.")
        
        if active_assets_count == 0: return
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in limited_params.items():
                kraken_symbol = SYMBOL_MAP.get(app_symbol)
                if not kraken_symbol: continue
                
                executor.submit(
                    self._execute_grid_logic, 
                    kraken_symbol, 
                    equity, 
                    params, 
                    active_assets_count
                )

    def _execute_grid_logic(self, symbol_str: str, equity: float, params: Dict, asset_count: int):
        time.sleep(random.uniform(0.1, 1.5))
        
        symbol_upper = symbol_str.upper()
        symbol_lower = symbol_str.lower()
        
        grid_lines = np.sort(np.array(params["line_prices"]))
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]

        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        pos_size, entry_price = self._get_position_details(symbol_upper)
        current_price = self._get_mark_price(symbol_upper)
        
        if current_price == 0: return

        is_flat = abs(pos_size) < specs["sizeStep"]
        
        bot_log(f"[{symbol_upper}] Price: {current_price} | Pos: {pos_size} @ {entry_price}")

        if is_flat:
            idx = np.searchsorted(grid_lines, current_price)
            line_below = grid_lines[idx-1] if idx > 0 else None
            line_above = grid_lines[idx] if idx < len(grid_lines) else None
            
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
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                        "size": qty, "limitPrice": price
                    })
            
            time.sleep(0.2) 

            if line_above:
                price = self._round_to_step(line_above, specs["tickSize"])
                if price > current_price:
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                        "size": qty, "limitPrice": price
                    })

        else:
            # --- IN POSITION: Manage Exits ---
            has_sl = False
            has_tp = False
            
            try:
                open_orders = self.kf.get_open_orders().get("openOrders", [])
                
                for o in open_orders:
                    # Robust Symbol Check
                    o_sym = o.get("symbol", "").lower()
                    if o_sym != symbol_lower:
                        continue
                        
                    # Extract Type safely (check 'orderType' OR 'type')
                    o_type = o.get("orderType", o.get("type", "")).lower()
                    o_reduce = o.get("reduceOnly", False)
                    o_trigger = o.get("triggerSignal", None) # Trigger Signal check
                    o_stop_px = o.get("stopPrice", None)
                    
                    # 1. Cleanup old grid limits (LMT without reduceOnly)
                    if o_type == "lmt" and not o_reduce:
                        try:
                            c_resp = self.kf.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})
                            bot_log(f"[{symbol_upper}] Cancelled Old Order {o['order_id']}: {c_resp}")
                        except Exception as e:
                            bot_log(f"[{symbol_upper}] Cancel Failed {o['order_id']}: {e}", level="error")
                        continue

                    # 2. Check for SL (stp OR trigger-based lmt)
                    is_sl_order = (
                        o_type == "stp" or 
                        o_stop_px is not None or 
                        o_trigger is not None
                    )

                    if is_sl_order:
                        has_sl = True
                        
                    # 3. Check for TP (lmt + reduceOnly + NO trigger)
                    is_tp_order = (
                        (o_type == "lmt" and o_reduce and o_trigger is None) or 
                        o_type == "take_profit"
                    )

                    if is_tp_order:
                        has_tp = True

            except Exception as e:
                bot_log(f"[{symbol_upper}] Order Check Error: {e}", level="error")

            if not has_sl or not has_tp:
                # Find current grid bands to log them
                idx = np.searchsorted(grid_lines, current_price)
                line_below = grid_lines[idx-1] if idx > 0 else None
                line_above = grid_lines[idx] if idx < len(grid_lines) else None

                self._place_bracket_orders(
                    symbol_lower, pos_size, entry_price, stop_pct, profit_pct, 
                    specs["tickSize"], line_below, line_above
                )

    def _place_bracket_orders(self, symbol: str, position_size: float, entry_price: float, 
                              sl_pct: float, tp_pct: float, tick_size: float,
                              lower_band: float=None, upper_band: float=None):
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

        bot_log(f"[{symbol.upper()}] Adding Brackets | Bands: {lower_band} - {upper_band} | SL: {sl_price} | TP: {tp_price}")

        # STOP LOSS - MARKET STOP (No limitPrice)
        try:
            sl_payload = {
                "orderType": "stp", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": sl_price, 
                # "limitPrice": sl_price,  <-- REMOVED to ensure Market Stop behavior
                "triggerSignal": "mark", 
                "reduceOnly": True
            }
            sl_resp = self.kf.send_order(sl_payload)
            bot_log(f"[{symbol.upper()}] SL Response: {sl_resp}")
            
            if "error" in sl_resp and sl_resp["error"]:
                 bot_log(f"[{symbol.upper()}] SL API Error: {sl_resp['error']}", level="error")
            time.sleep(0.3)
        except Exception as e:
            bot_log(f"[{symbol.upper()}] SL Failed: {e}", level="error")

        # TAKE PROFIT
        try:
            tp_resp = self.kf.send_order({
                "orderType": "lmt", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "limitPrice": tp_price, 
                "reduceOnly": True
            })
            if "error" in tp_resp and tp_resp["error"]:
                 bot_log(f"[{symbol.upper()}] TP API Error: {tp_resp['error']}", level="error")
        except Exception as e:
            bot_log(f"[{symbol.upper()}] TP Failed: {e}", level="error")

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
