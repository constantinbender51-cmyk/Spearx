#!/usr/bin/env python3
"""
Octopus Grid: Execution Engine for GA Grid Strategy
- Source: Connects to app.py endpoint (/api/parameters)
- Logic: Grid Entry (Nearest Lines) + Dynamic SL/TP
- Schedule: Entry (1 min), Protection & Cleanup (3 sec)
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
        self.active_params = {}
        self.param_lock = threading.Lock()

    def initialize(self):
        bot_log("--- Initializing Octopus Grid Bot (Fast Protect & Cleanup) ---")
        
        self._fetch_instrument_specs()
        
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                bot_log(f"API Error: {acc}", level="error")
                sys.exit(1)
            else:
                bot_log("API Connection Successful.")

            bot_log("Startup: Canceling all open orders for mapped assets...")
            unique_symbols = set(SYMBOL_MAP.values())
            
            for i, sym in enumerate(unique_symbols):
                try:
                    self.kf.cancel_all_orders({"symbol": sym.lower()})
                    time.sleep(1.0) 
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

    def run(self):
        bot_log("Bot started. Running Dual-Loop: Entries (1m), Exits (3s).")
        last_entry_run = 0

        while True:
            cycle_start = time.time()
            
            # --- 1. Fast Loop: Exit Monitor, Protection & Cleanup ---
            self._monitor_exits()
            
            # --- 2. Slow Loop: Entry Grid Logic ---
            dt_now = datetime.now(timezone.utc)
            if dt_now.second >= 5 and dt_now.second < 10:
                if cycle_start - last_entry_run > 45:
                    bot_log(f">>> ENTRY TRIGGER: {dt_now.strftime('%H:%M:%S')} <<<")
                    self._process_entry_cycle()
                    last_entry_run = time.time()
            
            time.sleep(3)

    # --- Fast Loop Logic (Exits & Cleanup) ---

    def _monitor_exits(self):
        """
        Fetches ALL data.
        If Position exists:
          1. CANCEL old Entry orders (Cleanup).
          2. PLACE SL/TP if missing (Protection).
        """
        if not self.active_params:
            return

        try:
            raw_pos = self.kf.get_open_positions()
            raw_ord = self.kf.get_open_orders()
            raw_tick = self.kf.get_tickers()
        except Exception as e:
            bot_log(f"Monitor Fetch Error: {e}", level="error")
            return

        pos_map = {p["symbol"].upper(): p for p in raw_pos.get("openPositions", [])}
        
        ord_map = {}
        for o in raw_ord.get("openOrders", []):
            s = o["symbol"].upper()
            if s not in ord_map: ord_map[s] = []
            ord_map[s].append(o)
            
        tick_map = {t["symbol"].upper(): float(t["markPrice"]) for t in raw_tick.get("tickers", [])}

        with self.param_lock:
            current_params = self.active_params.copy()

        for app_symbol, params in current_params.items():
            kraken_symbol = SYMBOL_MAP.get(app_symbol)
            if not kraken_symbol: continue
            
            sym_upper = kraken_symbol.upper()
            
            # We have a position?
            if sym_upper in pos_map:
                p_data = pos_map[sym_upper]
                size = float(p_data["size"])
                if p_data["side"] == "short": size = -size
                entry = float(p_data["price"])
                
                current_price = tick_map.get(sym_upper, 0.0)
                if current_price == 0: continue

                self._manage_active_position(
                    kraken_symbol, 
                    size, 
                    entry, 
                    current_price, 
                    ord_map.get(sym_upper, []), 
                    params
                )

    def _manage_active_position(self, symbol_str: str, pos_size: float, entry_price: float, 
                                current_price: float, open_orders: List, params: Dict):
        """
        1. Identifies and cancels 'Stale' Entry orders (unfilled limits).
        2. Checks for and places missing SL/TP.
        """
        symbol_lower = symbol_str.lower()
        symbol_upper = symbol_str.upper()
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]
        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        has_sl = False
        has_tp = False
        stale_entry_ids = []

        # Analyze existing orders
        for o in open_orders:
            o_type = o.get("orderType", o.get("type", "")).lower()
            o_reduce = o.get("reduceOnly", False)
            o_trigger = o.get("triggerSignal", None)
            o_stop_px = o.get("stopPrice", None)
            o_id = o.get("order_id")
            
            # DEFINITION: Stale Entry Order
            # It is a Limit order, NOT reduceOnly, and has NO trigger signal.
            # If we are in a position, these should NOT exist.
            is_entry_order = (o_type == "lmt" and not o_reduce and o_trigger is None)
            if is_entry_order:
                stale_entry_ids.append(o_id)

            # Check for SL (stp OR trigger-based)
            is_sl_order = (o_type == "stp" or o_stop_px is not None or o_trigger is not None)
            if is_sl_order: has_sl = True
            
            # Check for TP (lmt + reduceOnly + NO trigger)
            is_tp_order = ((o_type == "lmt" and o_reduce and o_trigger is None) or o_type == "take_profit")
            if is_tp_order: has_tp = True

        # --- STEP 1: CLEANUP (Cancel Stale Entries) ---
        if stale_entry_ids:
            bot_log(f"[{symbol_upper}] Position detected. Cleaning up {len(stale_entry_ids)} stale entry orders.")
            for oid in stale_entry_ids:
                try:
                    self.kf.cancel_order({"order_id": oid, "symbol": symbol_lower})
                except Exception as e:
                    bot_log(f"[{symbol_upper}] Cleanup Failed ID {oid}: {e}", level="warning")

        # --- STEP 2: PROTECTION (Ensure SL/TP) ---
        if not has_sl or not has_tp:
            self._place_bracket_orders(
                symbol_lower, pos_size, entry_price, current_price,
                stop_pct, profit_pct, specs["tickSize"]
            )

    def _place_bracket_orders(self, symbol: str, position_size: float, entry_price: float, current_price: float,
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

        bot_log(f"[{symbol.upper()}] PROTECTION MISSING. Placing Brackets | SL: {sl_price} | TP: {tp_price}")

        # Place SL
        try:
            self.kf.send_order({
                "orderType": "stp", "symbol": symbol, "side": side, "size": abs_size, 
                "stopPrice": sl_price, "triggerSignal": "mark", "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] SL Placement Failed: {e}", level="error")

        # Place TP
        try:
            self.kf.send_order({
                "orderType": "lmt", "symbol": symbol, "side": side, "size": abs_size, 
                "limitPrice": tp_price, "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] TP Placement Failed: {e}", level="error")

    # --- Slow Loop Logic (Entries) ---

    def _process_entry_cycle(self):
        new_params = self.fetcher.fetch_all_params()
        if not new_params:
            return
        
        with self.param_lock:
            self.active_params = new_params
        
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            else:
                first_acc = list(acc.get("accounts", {}).values())[0]
                equity = float(first_acc.get("marginEquity", 0))
                
            if equity <= 0: return
        except Exception:
            return

        limited_keys = list(new_params.keys())[:TEST_ASSET_LIMIT]
        limited_params = {k: new_params[k] for k in limited_keys}
        active_assets_count = len(limited_params)
        
        if active_assets_count == 0: return
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in limited_params.items():
                kraken_symbol = SYMBOL_MAP.get(app_symbol)
                if not kraken_symbol: continue
                
                executor.submit(
                    self._execute_entry_logic, 
                    kraken_symbol, 
                    equity, 
                    params, 
                    active_assets_count
                )

    def _execute_entry_logic(self, symbol_str: str, equity: float, params: Dict, asset_count: int):
        time.sleep(random.uniform(0.1, 1.5))
        
        symbol_upper = symbol_str.upper()
        symbol_lower = symbol_str.lower()
        
        grid_lines = np.sort(np.array(params["line_prices"]))
        stop_pct = params.get("stop_percent", 0.01)
        profit_pct = params.get("profit_percent", 0.01)

        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        pos_size, _ = self._get_position_details(symbol_upper)
        current_price = self._get_mark_price(symbol_upper)
        
        if current_price == 0: return

        idx = np.searchsorted(grid_lines, current_price)
        line_below = grid_lines[idx-1] if idx > 0 else None
        line_above = grid_lines[idx] if idx < len(grid_lines) else None

        is_flat = abs(pos_size) < specs["sizeStep"]
        
        if is_flat:
            bot_log(f"[{symbol_upper}] Flat. Placing Grid Limits. Px: {current_price}")
            
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
                    # Calculate Stop/TP based on Entry Price
                    sl_price = self._round_to_step(price * (1 - stop_pct), specs["tickSize"])
                    tp_price = self._round_to_step(price * (1 + profit_pct), specs["tickSize"])

                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                        "size": qty, "limitPrice": price,
                        "stopLoss": {"stopPrice": sl_price, "triggerSignal": "mark"},
                        "takeProfit": {"limitPrice": tp_price, "triggerSignal": "mark"}
                    })
            
            time.sleep(0.3)

            if line_above:
                price = self._round_to_step(line_above, specs["tickSize"])
                if price > current_price:
                    # Calculate Stop/TP based on Entry Price
                    sl_price = self._round_to_step(price * (1 + stop_pct), specs["tickSize"])
                    tp_price = self._round_to_step(price * (1 - profit_pct), specs["tickSize"])

                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                        "size": qty, "limitPrice": price,
                        "stopLoss": {"stopPrice": sl_price, "triggerSignal": "mark"},
                        "takeProfit": {"limitPrice": tp_price, "triggerSignal": "mark"}
                    })
        # If not flat, do nothing. The fast loop handles cleanup/protection.

    # --- Helpers ---

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
        except Exception:
            return 0.0, 0.0

    def _get_mark_price(self, kf_symbol_upper: str) -> float:
        try:
            tickers = self.kf.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == kf_symbol_upper:
                    return float(t["markPrice"])
            return 0.0
        except Exception:
            return 0.0

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
