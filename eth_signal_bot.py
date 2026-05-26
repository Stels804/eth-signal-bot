#!/usr/bin/env python3
"""
Сигнальный робот ETH/USDT для Railway
НЕ ТОРГУЕТ — только сигналы в Telegram
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime
from typing import Optional, Tuple

import pytz
import websockets
import aiohttp
from pybit.unified_trading import HTTP

# ========== НАСТРОЙКИ ==========
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Не заданы TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID")

TZ_LOCAL = os.environ.get("TZ_LOCAL", "Europe/Moscow")
TRADING_HOURS_START = int(os.environ.get("TRADING_HOURS_START", "10"))
TRADING_HOURS_END = int(os.environ.get("TRADING_HOURS_END", "18"))
WEEKEND_TRADING = os.environ.get("WEEKEND_TRADING", "False").lower() == "true"

UPDATE_INTERVAL = 10
REST_INTERVAL = 30

IMBALANCE_LONG = 0.35
IMBALANCE_SHORT = 0.65
FUNDING_LONG = -0.00015
FUNDING_SHORT = 0.00015
CVD_CONSECUTIVE = 3
OI_DROP_THRESHOLD = -0.05

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class EthSignalBot:
    def __init__(self):
        self.session = HTTP(testnet=False)
        self.trades = deque(maxlen=1000)
        self.cvd_history = deque(maxlen=100)
        self.cvd_down_count = 0
        self.cvd_up_count = 0
        self.orderbook_bids = []
        self.orderbook_asks = []
        self.imbalance = 0.5
        self.oi_history = deque(maxlen=10)
        self.oi_drop_detected = False
        self.funding_rate = 0.0
        self.current_price = 0.0
        self.signal_state: Optional[str] = None
        self.entry_price = 0.0
        self.entry_time: Optional[datetime] = None
        self.ws_trade_task: Optional[asyncio.Task] = None
        self.ws_orderbook_task: Optional[asyncio.Task] = None
        self.last_rest_time = 0
        self.last_hourly = 0
        self.startup_sent = False
        self.running = True

    def get_local_time(self) -> datetime:
        return datetime.now(pytz.timezone(TZ_LOCAL))

    def is_trading_allowed(self) -> bool:
        now_local = self.get_local_time()
        if not WEEKEND_TRADING and now_local.weekday() in (5, 6):
            return False
        return TRADING_HOURS_START <= now_local.hour < TRADING_HOURS_END

    async def send_telegram(self, message: str) -> None:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                        logger.error(f"Telegram error: {await resp.text()}" if resp.status != 200 else None)
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    async def send_signal(self, signal_type: str, price: float, reason: str) -> None:
        now_local = self.get_local_time()
        time_str = now_local.strftime("%Y-%m-%d %H:%M:%S")

        if signal_type == "LONG":
            self.signal_state = "long"
            self.entry_price = price
            self.entry_time = now_local
            message = f"🟢 LONG СИГНАЛ\nЦена: {price:.2f}\nПричина: {reason}\nВремя: {time_str}"
        elif signal_type == "SHORT":
            self.signal_state = "short"
            self.entry_price = price
            self.entry_time = now_local
            message = f"🔴 SHORT СИГНАЛ\nЦена: {price:.2f}\nПричина: {reason}\nВремя: {time_str}"
        elif signal_type == "CLOSE_LONG":
            pnl = ((price - self.entry_price) / self.entry_price) * 100 if self.entry_price else 0
            message = f"🟠 CLOSE LONG\nЦена: {price:.2f}\nПричина: {reason}\nPnL: {pnl:+.2f}%\nВремя: {time_str}"
            self.signal_state = None
        elif signal_type == "CLOSE_SHORT":
            pnl = ((self.entry_price - price) / self.entry_price) * 100 if self.entry_price else 0
            message = f"🔵 CLOSE SHORT\nЦена: {price:.2f}\nПричина: {reason}\nPnL: {pnl:+.2f}%\nВремя: {time_str}"
            self.signal_state = None
        else:
            return

        await self.send_telegram(message)
        logger.info(f"Сигнал: {signal_type}")

    async def send_hourly_status(self) -> None:
        status = "🟢 LONG" if self.signal_state == "long" else "🔴 SHORT" if self.signal_state == "short" else "⚪ Нет сигнала"
        message = (f"📊 Часовая сводка ETH/USDT\n\n"
                   f"Сигнал: {status}\nЦена: {self.current_price:.2f}\n"
                   f"Imbalance: {self.imbalance:.3f}\nFunding: {self.funding_rate*100:.4f}%\n"
                   f"OI падение: {'✅' if self.oi_drop_detected else '❌'}")
        await self.send_telegram(message)

    async def fetch_open_interest(self) -> float:
        try:
            response = self.session.get_open_interest(category="linear", symbol="ETHUSDT", interval="5min")
            if response and response.get("retCode") == 0:
                oi_list = response.get("result", {}).get("list", [])
                if oi_list:
                    return float(oi_list[0].get("openInterest", 0))
        except Exception as e:
            logger.error(f"OI fetch error: {e}")
        return 0.0

    async def fetch_funding_rate(self) -> float:
        try:
            response = self.session.get_tickers(category="linear", symbol="ETHUSDT")
            if response and response.get("retCode") == 0:
                tickers = response.get("result", {}).get("list", [])
                if tickers:
                    return float(tickers[0].get("fundingRate", 0))
        except Exception as e:
            logger.error(f"Funding fetch error: {e}")
        return 0.0

    async def update_rest_data(self) -> None:
        oi = await self.fetch_open_interest()
        if oi > 0:
            self.oi_history.append(oi)
            if len(self.oi_history) >= 3:
                prev_oi = self.oi_history[-3]
                if prev_oi > 0:
                    self.oi_drop_detected = (oi - prev_oi) / prev_oi < OI_DROP_THRESHOLD
        
        fr = await self.fetch_funding_rate()
        if fr != 0:
            self.funding_rate = fr

    async def ws_trade_handler(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": ["publicTrade.ETHUSDT"]}))
                    logger.info("WebSocket сделок подключен")
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            if "data" in data and data.get("topic") == "publicTrade.ETHUSDT":
                                for trade in data.get("data", []):
                                    self.trades.append({
                                        'price': float(trade['price']),
                                        'size': float(trade['size']),
                                        'side': trade['side']
                                    })
                                    self.current_price = float(trade['price'])
                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            break
            except Exception as e:
                logger.error(f"WS trade error: {e}")
                await asyncio.sleep(5)

    async def ws_orderbook_handler(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": ["orderbook.1.ETHUSDT"]}))
                    logger.info("WebSocket стакана подключен")
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            if "data" in data and "orderbook" in data.get("topic", ""):
                                ob = data["data"]
                                if "b" in ob and "a" in ob:
                                    self.orderbook_bids = [[float(b[0]), float(b[1])] for b in ob["b"][:10]]
                                    self.orderbook_asks = [[float(a[0]), float(a[1])] for a in ob["a"][:10]]
                                    self._update_imbalance()
                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            break
            except Exception as e:
                logger.error(f"WS orderbook error: {e}")
                await asyncio.sleep(5)

    def _update_imbalance(self) -> None:
        if not self.orderbook_bids or not self.orderbook_asks:
            return
        bid_vol = sum(b[1] for b in self.orderbook_bids[:3])
        ask_vol = sum(a[1] for a in self.orderbook_asks[:3])
        self.imbalance = bid_vol / (bid_vol + ask_vol) if ask_vol > 0 else 0.5

    def _update_cvd(self) -> None:
        if not self.trades:
            return
        delta = sum(t['size'] if t['side'] == 'Buy' else -t['size'] for t in self.trades)
        self.cvd_history.append(delta if len(self.cvd_history) == 0 else self.cvd_history[-1] + delta)
        if len(self.cvd_history) >= 2:
            if self.cvd_history[-1] < self.cvd_history[-2]:
                self.cvd_down_count += 1
                self.cvd_up_count = 0
            elif self.cvd_history[-1] > self.cvd_history[-2]:
                self.cvd_up_count += 1
                self.cvd_down_count = 0
        self.trades.clear()

    def _check_long_signal(self) -> Tuple[bool, str]:
        if self.signal_state:
            return False, ""
        ok = (self.cvd_down_count >= CVD_CONSECUTIVE and 
              self.imbalance < IMBALANCE_LONG and 
              self.funding_rate < FUNDING_LONG and 
              self.oi_drop_detected)
        reason = f"CVD падает {self.cvd_down_count} | Imbalance {self.imbalance:.3f} | Funding {self.funding_rate*100:.4f}% | OI падает"
        return ok, reason if ok else ""

    def _check_short_signal(self) -> Tuple[bool, str]:
        if self.signal_state:
            return False, ""
        ok = (self.cvd_up_count >= CVD_CONSECUTIVE and 
              self.imbalance > IMBALANCE_SHORT and 
              self.funding_rate > FUNDING_SHORT and 
              self.oi_drop_detected)
        reason = f"CVD растёт {self.cvd_up_count} | Imbalance {self.imbalance:.3f} | Funding {self.funding_rate*100:.4f}% | OI падает"
        return ok, reason if ok else ""

    def _check_close_long(self) -> Tuple[bool, str]:
        if self.signal_state != "long":
            return False, ""
        if self.cvd_down_count >= 2 or self.imbalance > IMBALANCE_SHORT or self.funding_rate > 0.0003:
            return True, "Разворот толпы"
        return False, ""

    def _check_close_short(self) -> Tuple[bool, str]:
        if self.signal_state != "short":
            return False, ""
        if self.cvd_up_count >= 2 or self.imbalance < IMBALANCE_LONG or self.funding_rate < -0.0003:
            return True, "Разворот толпы"
        return False, ""

    async def main_loop(self):
        while self.running:
            try:
                now = time.time()
                if not self.startup_sent:
                    await self.send_telegram(
                        f"✅ Сигнальный робот ETH/USDT запущен\n"
                        f"Часовой пояс: {TZ_LOCAL}\n"
                        f"Торговые часы: {TRADING_HOURS_START}:00-{TRADING_HOURS_END}:00\n\n"
                        f"⚠️ Робот НЕ ТОРГУЕТ — только сигналы"
                    )
                    self.startup_sent = True
                
                if now - self.last_rest_time >= REST_INTERVAL:
                    self.last_rest_time = now
                    await self.update_rest_data()
                
                current_hour = int(now / 3600)
                if current_hour != self.last_hourly:
                    self.last_hourly = current_hour
                    await self.send_hourly_status()
                
                if not self.is_trading_allowed():
                    await asyncio.sleep(60)
                    continue
                
                if len(self.trades) > 0:
                    self._update_cvd()
                
                if self.signal_state == "long":
                    close, reason = self._check_close_long()
                    if close:
                        await self.send_signal("CLOSE_LONG", self.current_price, reason)
                elif self.signal_state == "short":
                    close, reason = self._check_close_short()
                    if close:
                        await self.send_signal("CLOSE_SHORT", self.current_price, reason)
                else:
                    long_signal, long_reason = self._check_long_signal()
                    if long_signal:
                        await self.send_signal("LONG", self.current_price, long_reason)
                    
                    short_signal, short_reason = self._check_short_signal()
                    if short_signal:
                        await self.send_signal("SHORT", self.current_price, short_reason)
                
                await asyncio.sleep(UPDATE_INTERVAL)
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        logger.info("Запуск сигнального робота ETH/USDT")
        self.ws_trade_task = asyncio.create_task(self.ws_trade_handler())
        self.ws_orderbook_task = asyncio.create_task(self.ws_orderbook_handler())
        await asyncio.sleep(3)
        await self.main_loop()

    def stop(self):
        self.running = False


async def shutdown(sig, bot):
    logger.info(f"Получен сигнал {sig.name}")
    bot.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    sys.exit(0)


async def main():
    bot = EthSignalBot()
    loop = asyncio.get_running_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig, bot)))
    await bot.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
