#!/usr/bin/env python3
"""
Octopus Trend: Execution Engine for ETH Trend Strategy
- Logic: Long ETH if BTC Price > 365-period BTC SMA, else Short ETH.
- Interval: 1 Hour
- Sizing: 1x Margin Equity
"""

import os
import sys
import time
import logging
import requests
import numpy as np

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

# Strategy Settings
TRADE_SYMBOL = "PF_ETHUSD"
SIGNAL_SYMBOL = "PF_XBTUSD"  # BTC
SMA_PERIOD = 365             # 365 hours
LEVERAGE = 1.0               # 1x Equity
UPDATE_INTERVAL = 3600       # 1 Hour

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("trend_octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OctopusTrend")

class OctopusTrendBot:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.min_size = 0.01
        self.tick_size = 0.01

    def initialize(self):
        logger.info("--- Initializing Octopus Trend Bot ---")
        try:
            # Check connection
            acc = self.kf.get_accounts()
            if "error" in acc:
                logger.error(f"API Error: {acc}")
                sys.exit(1)
            
            # Fetch specs for precision
            self._fetch_specs()
            
        except Exception as e:
            logger.error(f"Startup Failed: {e}")
            sys.exit(1)

    def _fetch_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    if inst["symbol"].upper() == TRADE_SYMBOL:
                        precision = inst.get("contractValueTradePrecision", 2)
                        self.min_size = 10 ** (-int(precision))
                        logger.info(f"ETH Specs | Min Size: {self.min_size}")
                        break
        except Exception as e:
            logger.warning(f"Spec fetch failed, using defaults: {e}")

    def get_btc_sma_state(self):
        """
        Fetches BTC 1h candles, calculates 365 SMA.
        Returns: (btc_price, sma_value, is_bullish)
        """
        try:
            # v3 candles: resolution 1h
            url = f"https://futures.kraken.com/derivatives/api/v3/candles?symbol={SIGNAL_SYMBOL}&candleType=trade&resolution=1h"
            resp = requests.get(url, timeout=10).json()
            
            if "candles" not in resp:
                logger.error("No candle data returned.")
                return None, None, None

            # Sort by time asc
            candles = sorted(resp["candles"], key=lambda x: x["time"])
            closes = np.array([float(c["close"]) for c in candles])

            if len(closes) < SMA_PERIOD:
                logger.warning(f"Insufficient Data: {len(closes)}/{SMA_PERIOD}")
                return None, None, None

            sma = np.mean(closes[-SMA_PERIOD:])
            current_price = closes[-1]
            
            # Check live price for most recent data point
            tickers = self.kf.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == SIGNAL_SYMBOL:
                    current_price = float(t["markPrice"])
                    break

            is_bullish = current_price > sma
            return current_price, sma, is_bullish

        except Exception as e:
            logger.error(f"Signal Logic Error: {e}")
            return None, None, None

    def get_equity(self):
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                return float(acc["accounts"]["flex"].get("marginEquity", 0))
            # Fallback for single collateral
            first = list(acc.get("accounts", {}).values())[0]
            return float(first.get("marginEquity", 0))
        except Exception:
            return 0.0

    def get_current_position(self):
        try:
            pos = self.kf.get_open_positions()
            for p in pos.get("openPositions", []):
                if p["symbol"].upper() == TRADE_SYMBOL:
                    size = float(p["size"])
                    return size if p["side"] == "long" else -size
            return 0.0
        except Exception:
            return 0.0

    def _round_size(self, size):
        steps = size / self.min_size
        return round(steps) * self.min_size

    def run(self):
        logger.info(f"Bot Running. Cycle: {UPDATE_INTERVAL}s. Target: {TRADE_SYMBOL}")
        
        while True:
            try:
                # 1. Equity Check
                equity = self.get_equity()
                if equity <= 0:
                    logger.error("Equity 0 or Fetch Fail.")
                    time.sleep(60)
                    continue

                # 2. Strategy Logic
                btc_price, sma, bull = self.get_btc_sma_state()
                if btc_price is None:
                    time.sleep(60)
                    continue

                logger.info(f"State | BTC: {btc_price:.2f} | SMA({SMA_PERIOD}): {sma:.2f} | Bias: {'LONG' if bull else 'SHORT'}")

                # 3. Sizing
                eth_tickers = self.kf.get_tickers()
                eth_price = 0.0
                for t in eth_tickers.get("tickers", []):
                    if t["symbol"].upper() == TRADE_SYMBOL:
                        eth_price = float(t["markPrice"])
                        break
                
                if eth_price == 0:
                    logger.error("ETH Price unavailable.")
                    continue

                # Target Notional = Equity * 1.0
                target_value = equity * LEVERAGE
                target_qty = target_value / eth_price
                
                if not bull:
                    target_qty = -target_qty

                target_qty = self._round_size(target_qty)

                # 4. Execution
                current_qty = self.get_current_position()
                diff = target_qty - current_qty
                
                # Check if change is significant (reduce churn)
                if abs(diff) < self.min_size:
                    logger.info("Position aligned. No trade.")
                else:
                    side = "buy" if diff > 0 else "sell"
                    logger.info(f"Rebalance | Curr: {current_qty} -> Targ: {target_qty} | Exec: {side} {abs(diff):.4f}")
                    
                    self.kf.send_order({
                        "orderType": "mkt",
                        "symbol": TRADE_SYMBOL.lower(),
                        "side": side,
                        "size": abs(diff)
                    })

            except Exception as e:
                logger.error(f"Loop Error: {e}")

            logger.info(f"Sleeping {UPDATE_INTERVAL/60:.1f} min...")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = OctopusTrendBot()
    bot.initialize()
    bot.run()
