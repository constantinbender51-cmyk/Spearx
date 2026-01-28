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
MAX_WORKERS = 5
LEVERAGE = 1

# Strategy Endpoint (The app.py server)
STRATEGY_URL = "https://live-trading-production.up.railway.app/api/parameters"

# Asset Mapping: App Symbol -> Kraken Futures Symbol
# Note: Kraken Futures usually follows PF_<Symbol>USD format.
# BTC is XBT on Kraken.
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
        Expected format from app.py when no symbol is specified: 
        { 
          "BTC": { "stop_percent": ..., "line_prices": ... },
          "ETH": { "stop_percent": ..., "line_prices": ... },
          ...
        }
        """
        bot_log(f"Fetching grid params from {STRATEGY_URL}...")
        try:
            resp = requests.get(STRATEGY_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                # Validate it's a dict of dicts
                if isinstance(data, dict):
                    valid_strategies = {}
                    for sym, params in data.items():
                        if "line_prices" in params and "stop_percent" in params:
                            valid_strategies[sym] = params
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
        except Exception as e:
            bot_log(f"API Connection Failed: {e}", level="error")
            sys.exit(1)

    def _fetch_instrument_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                # We want to cache specs for ALL potential mapped symbols
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
                time.sleep(50) # Sleep to pass the trigger window
            time.sleep(1)

    def _process_cycle(self):
        # 1. Fetch ALL Strategy Parameters
        all_params = self.fetcher.fetch_all_params()
        if not all_params:
            bot_log("No params received from server. Skipping cycle.", level="warning")
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

        # 3. Determine Execution Size per Asset
        # To avoid overleveraging, we split equity among active strategies.
        # We use a conservative 90% of equity divided by the number of active assets.
        active_assets_count = len(all_params)
        if active_assets_count == 0: return
        
        # 4. Execute Logic for Each Asset
        # Using ThreadPool to prevent IO blocking (HTTP requests) from delaying other assets 
        # within the 5-second trigger window.
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in all_params.items():
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
        
        # Extract Params
        grid_lines = np.sort(np.array(params["line_prices"]))
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]

        specs = self.instrument_specs.get(symbol_upper)
        if not specs: 
            bot_log(f"No specs for {symbol_upper} - skipping.", level="error")
            return

        # Get State
        pos_size, entry_price = self._get_position_details(symbol_upper)
        current_price = self._get_mark_price(symbol_upper)
        
        if current_price == 0: return

        # Calculate Position Status
        is_flat = abs(pos_size) < specs["sizeStep"]
        
        bot_log(f"[{symbol_upper}] Price: {current_price} | Pos: {pos_size} @ {entry_price}")

        # --- LOGIC BRANCH ---
        
        if is_flat:
            # --- 1. FLAT: Place Grid Entry Orders ---
            
            # Find nearest lines
            idx = np.searchsorted(grid_lines, current_price)
            
            # Line Below (Buy Trigger)
            line_below = grid_lines[idx-1] if idx > 0 else None
            
            # Line Above (Sell Trigger)
            line_above = grid_lines[idx] if idx < len(grid_lines) else None
            
            # Calculate Order Size (Split Equity Logic)
            # Allocation = (Equity * 0.95) / Count
            # Unit per trade = Allocation * LEVERAGE * 0.20 (Conservative entry within allocation)
            safe_equity = equity * 0.95
            allocation_per_asset = safe_equity / max(1, asset_count)
            unit_usd = (allocation_per_asset * LEVERAGE) * 0.20 
            
            qty = unit_usd / current_price
            qty = self._round_to_step(qty, specs["sizeStep"])

            if qty < specs["sizeStep"]:
                bot_log(f"[{symbol_upper}] Qty too small ({qty}).", level="warning")
                return

            # Cancel Existing Orders (Fresh Start for Grid)
            try: self.kf.cancel_all_orders({"symbol": symbol_lower})
            except: pass
            
            # Place LONG Entry (Limit Buy at line_below)
            if line_below:
                price = self._round_to_step(line_below, specs["tickSize"])
                if price < current_price:
                    bot_log(f"[{symbol_upper}] Placing BUY LMT @ {price} (Qty: {qty})")
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                        "size": qty, "limitPrice": price
                    })
            
            # Place SHORT Entry (Limit Sell at line_above)
            if line_above:
                price = self._round_to_step(line_above, specs["tickSize"])
                if price > current_price:
                    bot_log(f"[{symbol_upper}] Placing SELL LMT @ {price} (Qty: {qty})")
                    self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                        "size": qty, "limitPrice": price
                    })

        else:
            # --- 2. IN POSITION: Manage Exits (SL/TP) ---
            
            # First, Cancel any lingering Entry Limits
            try:
                open_orders = self.kf.get_open_orders().get("openOrders", [])
                for o in open_orders:
                    if o["symbol"] == symbol_lower and o["orderType"] == "lmt":
                        self.kf.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})
            except Exception as e:
                bot_log(f"[{symbol_upper}] Cleanup error: {e}", level="warning")

            # Check for existing protective orders
            has_sl = False
            has_tp = False
            
            try:
                open_orders = self.kf.get_open_orders().get("openOrders", [])
                for o in open_orders:
                    if o["symbol"] == symbol_lower:
                        if o["orderType"] == "stp": has_sl = True
                        if o["orderType"] == "take_profit": has_tp = True
            except: pass

            # If missing protection, place it
            if not has_sl or not has_tp:
                self._place_bracket_orders(symbol_lower, pos_size, entry_price, stop_pct, profit_pct, specs["tickSize"])

    def _place_bracket_orders(self, symbol: str, position_size: float, entry_price: float, 
                              sl_pct: float, tp_pct: float, tick_size: float):
        """
        Places OCO-like Stop Loss and Take Profit orders.
        """
        is_long = position_size > 0
        side = "sell" if is_long else "buy"
        abs_size = abs(position_size)

        # Calculate Prices
        if is_long:
            sl_price = entry_price * (1 - sl_pct)
            tp_price = entry_price * (1 + tp_pct)
        else:
            sl_price = entry_price * (1 + sl_pct)
            tp_price = entry_price * (1 - tp_pct)

        sl_price = self._round_to_step(sl_price, tick_size)
        tp_price = self._round_to_step(tp_price, tick_size)

        bot_log(f"[{symbol.upper()}] Adding Brackets | Entry: {entry_price} | SL: {sl_price} | TP: {tp_price}")

        # Send Stop Loss
        try:
            self.kf.send_order({
                "orderType": "stp", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": sl_price,
                "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] SL Failed: {e}", level="error")

        # Send Take Profit
        try:
            self.kf.send_order({
                "orderType": "take_profit", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": tp_price, 
                "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] TP Failed: {e}", level="error")

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
