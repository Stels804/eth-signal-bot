#!/usr/bin/env python3
"""
СИГНАЛЬНЫЙ РОБОТ ДЛЯ ETH/USDT НА BYBIT
НИЧЕГО НЕ ТОРГУЕТ — ТОЛЬКО СИГНАЛЫ В TELEGRAM

Исправления v2:
1. Long/Short Ratio — правильный эндпоинт /v5/market/account-ratio
2. Telegram-команды (/ping, /status, /signals, /stats) — polling getUpdates
3. ATR исправлен — True Range через историю high/low/close
4. update_result() теперь вызывается из цикла проверки результатов
5. Конфликт имён signal/sig в shutdown() исправлен

Установка:
pip install pybit pandas numpy websockets pytz aiohttp ta

Запуск:
python eth_signal_bot.py
"""

import asyncio
import csv
import json
import logging
import os
import signal as signal_module
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd
import numpy as np
import pytz
from pybit.unified_trading import WebSocket, HTTP
from aiohttp import ClientSession, web

# ========== НАСТРОЙКИ (ОБЯЗАТЕЛЬНО ИЗМЕНИТЬ) ==========
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"  # Токен от @BotFather
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"      # ID чата для уведомлений

# Часовой пояс
TZ_LOCAL = "Europe/Moscow"

# Расписание торговли (по ЛОКАЛЬНОМУ времени)
TRADING_HOURS_START = 10
TRADING_HOURS_END = 18
WEEKEND_TRADING = False

# Список новостей (UTC)
NEWS_TIMES: List[Tuple[int, int, int, int]] = []

# Технические параметры
CVD_WINDOW = 100
IMBALANCE_LEVELS = 3
UPDATE_INTERVAL = 10
REST_INTERVAL = 30

# Пороги индикаторов
IMBALANCE_LONG = 0.35
IMBALANCE_SHORT = 0.65
FUNDING_LONG = -0.00015
FUNDING_SHORT = 0.00015
FUNDING_CLOSE_LONG = 0.0003
FUNDING_CLOSE_SHORT = -0.0003
CVD_CONSECUTIVE = 3
OI_DROP_THRESHOLD = -0.05

# ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ
ATR_PERIOD = 14
MIN_ATR_PERCENT = 0.3  # Минимальная волатильность 0.3%
RSI_PERIOD = 14
RSI_LONG = 30          # RSI < 30 для LONG
RSI_SHORT = 70         # RSI > 70 для SHORT

# Healthcheck HTTP порт
HEALTHCHECK_PORT = 8080

# CSV файл для логирования
CSV_FILENAME = "signals_log.csv"

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("signals.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CSVLogger:
    """CSV логирование сигналов"""

    def __init__(self, filename: str):
        self.filename = filename
        self.signals = []

        if not os.path.exists(filename):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'signal_type', 'price', 'reason',
                    'cvd', 'imbalance', 'funding_rate', 'oi_drop',
                    'rsi', 'atr_percent', 'long_short_ratio',
                    'actual_result_after_1h', 'actual_result_after_4h'
                ])

    def log_signal(self, signal_type: str, price: float, reason: str,
                   cvd: float, imbalance: float, funding_rate: float,
                   oi_drop: bool, rsi: float, atr_percent: float,
                   long_short_ratio: float) -> None:
        """Записывает сигнал в CSV"""
        ts = datetime.now().isoformat()
        row = [
            ts, signal_type, price, reason,
            cvd, imbalance, funding_rate, oi_drop,
            rsi, atr_percent, long_short_ratio,
            '', ''
        ]

        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(row)

        self.signals.append({
            'timestamp': datetime.now(),
            'signal_type': signal_type,
            'price': price,
            'ts_iso': ts,
            'checked_1h': False,
            'checked_4h': False,
        })
        self.cleanup_old()

    def cleanup_old(self) -> None:
        cutoff = datetime.now() - timedelta(days=7)
        self.signals = [s for s in self.signals if s['timestamp'] > cutoff]

    # ИСПРАВЛЕНİЕ 4: update_result теперь корректно находит строку по ts_iso
    def update_result(self, ts_iso: str, hours: int, result_pct: float) -> None:
        """Обновляет результат сигнала через N часов"""
        try:
            rows = []
            with open(self.filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = list(reader)

            for row in rows:
                if row[0] == ts_iso:
                    col_idx = 11 if hours == 1 else 12
                    if len(row) > col_idx:
                        row[col_idx] = f"{result_pct:.2f}%"
                    break

            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
        except Exception as e:
            logger.error(f"Failed to update CSV result: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        if len(self.signals) < 5:
            return {"message": "Недостаточно сигналов для статистики (нужно минимум 5)"}

        long_signals = [s for s in self.signals if s['signal_type'] == 'LONG']
        short_signals = [s for s in self.signals if s['signal_type'] == 'SHORT']

        return {
            "total_signals": len(self.signals),
            "long_signals": len(long_signals),
            "short_signals": len(short_signals),
            "last_signal": self.signals[-1]['timestamp'].isoformat() if self.signals else None,
        }


class EthSignalBot:
    """Сигнальный робот для ETH/USDT — НИЧЕГО НЕ ТОРГУЕТ"""

    def __init__(self):
        self.session = HTTP(testnet=False)

        self.trades: deque = deque(maxlen=1000)
        self.cvd_history: deque = deque(maxlen=CVD_WINDOW)
        self.cvd_down_count: int = 0
        self.cvd_up_count: int = 0
        self.current_cvd: float = 0.0

        self.orderbook: Dict[str, list] = {"bids": [], "asks": []}
        self.imbalance: float = 0.5

        self.oi_history: deque = deque(maxlen=10)
        self.oi_drop_detected: bool = False

        self.funding_rate: float = 0.0
        self.current_price: float = 0.0
        self.long_short_ratio: float = 0.5

        # ИСПРАВЛЕНИЕ 3: храним (high, low, close) для правильного ATR
        self.candle_history: deque = deque(maxlen=200)  # каждый элемент: (high, low, close)
        self.price_history: deque = deque(maxlen=200)   # только close для RSI

        self.rsi: float = 50.0
        self.atr: float = 0.0
        self.atr_percent: float = 0.0

        self.signal_state: Optional[str] = None
        self.entry_price: float = 0.0
        self.entry_time: Optional[datetime] = None

        self.ws_trade: Optional[WebSocket] = None
        self.ws_orderbook: Optional[WebSocket] = None

        self.last_rest_time: float = 0
        self.last_hourly: int = 0
        self.last_warning_minute: int = -1
        self.last_result_check: float = 0

        # ИСПРАВЛЕНИЕ 2: для Telegram polling
        self.last_update_id: int = 0

        self.startup_sent: bool = False
        self.running: bool = True

        self.csv_logger = CSVLogger(CSV_FILENAME)

        self.web_app = None
        self.web_runner = None

    # ========== РАБОТА СО ВРЕМЕНЕМ ==========

    def get_local_time(self) -> datetime:
        return datetime.now(pytz.timezone(TZ_LOCAL))

    def get_minutes_to_session_end(self) -> int:
        now_local = self.get_local_time()
        end_time_minutes = TRADING_HOURS_END * 60
        now_minutes = now_local.hour * 60 + now_local.minute
        return end_time_minutes - now_minutes

    def is_trading_allowed(self) -> bool:
        now_local = self.get_local_time()

        if not WEEKEND_TRADING and now_local.weekday() in (5, 6):
            return False

        current_hour = now_local.hour
        if current_hour < TRADING_HOURS_START or current_hour >= TRADING_HOURS_END:
            return False

        now_utc = datetime.now(pytz.UTC)
        for month, day, hour, minute in NEWS_TIMES:
            news_time = datetime(now_utc.year, month, day, hour, minute, tzinfo=pytz.UTC)
            time_diff = (now_utc - news_time).total_seconds() / 60
            if -15 <= time_diff <= 10:
                return False

        return True

    # ========== ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ ==========

    def calculate_rsi(self) -> float:
        if len(self.price_history) < RSI_PERIOD + 1:
            return 50.0

        prices = list(self.price_history)
        gains, losses = [], []

        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]
            gains.append(diff if diff >= 0 else 0)
            losses.append(-diff if diff < 0 else 0)

        gains = gains[-RSI_PERIOD:]
        losses = losses[-RSI_PERIOD:]

        avg_gain = sum(gains) / RSI_PERIOD
        avg_loss = sum(losses) / RSI_PERIOD

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    # ИСПРАВЛЕНИЕ 3: True Range рассчитывается правильно через (high, low, prev_close)
    def calculate_atr(self) -> float:
        """Рассчитывает ATR через истинный True Range: max(H-L, |H-PC|, |L-PC|)"""
        if len(self.candle_history) < ATR_PERIOD + 1:
            return 0.0

        candles = list(self.candle_history)
        true_ranges = []

        for i in range(1, len(candles)):
            high = candles[i][0]
            low = candles[i][1]
            prev_close = candles[i - 1][2]

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)

        if len(true_ranges) >= ATR_PERIOD:
            return sum(true_ranges[-ATR_PERIOD:]) / ATR_PERIOD

        return 0.0

    def update_ta_indicators(self) -> None:
        if len(self.price_history) >= 5:
            self.rsi = self.calculate_rsi()
            self.atr = self.calculate_atr()
            if self.current_price > 0:
                self.atr_percent = (self.atr / self.current_price) * 100
            else:
                self.atr_percent = 0.0

    # ========== TELEGRAM ==========

    async def send_telegram(self, message: str, reply_markup: dict = None) -> None:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)

        try:
            async with ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"Telegram error: {await resp.text()}")
        except Exception as e:
            logger.error(f"Failed to send Telegram: {e}")

    # ИСПРАВЛЕНИЕ 2: polling входящих сообщений (команды /ping, /status и др.)
    async def poll_telegram_commands(self) -> None:
        """Получает и обрабатывает входящие команды из Telegram"""
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"timeout": 5, "offset": self.last_update_id + 1}

        try:
            async with ClientSession() as session:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            for update in data.get("result", []):
                self.last_update_id = update["update_id"]

                # Обработка текстовых команд
                message = update.get("message", {})
                text = message.get("text", "")
                chat_id = str(message.get("chat", {}).get("id", ""))

                # Отвечаем только в наш чат
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                if text.startswith("/ping"):
                    await self.send_telegram("🏓 Pong! Бот работает.")

                elif text.startswith("/status"):
                    await self._send_status_message()

                elif text.startswith("/signals"):
                    await self._send_recent_signals()

                elif text.startswith("/stats"):
                    stats = self.csv_logger.get_statistics()
                    if isinstance(stats, dict) and "message" in stats:
                        await self.send_telegram(stats["message"])
                    else:
                        msg = (
                            f"📊 <b>Статистика сигналов</b>\n\n"
                            f"Всего: {stats.get('total_signals', 0)}\n"
                            f"LONG: {stats.get('long_signals', 0)}\n"
                            f"SHORT: {stats.get('short_signals', 0)}\n"
                            f"Последний: {stats.get('last_signal', 'нет')}"
                        )
                        await self.send_telegram(msg)

                elif text.startswith("/help"):
                    await self.send_telegram(
                        "🤖 <b>Справка по боту</b>\n\n"
                        "/ping — проверка работы\n"
                        "/status — текущий статус и индикаторы\n"
                        "/signals — последние сигналы\n"
                        "/stats — статистика\n"
                        "/help — это сообщение\n\n"
                        "⚠️ Бот НЕ ТОРГУЕТ — только сигналы."
                    )

                # Обработка callback_query от inline-кнопок
                callback = update.get("callback_query", {})
                if callback:
                    await self.handle_callback_query(callback.get("data", ""))

        except Exception as e:
            logger.error(f"Telegram polling error: {e}")

    async def _send_status_message(self) -> None:
        """Текущий статус индикаторов"""
        now_local = self.get_local_time()

        if self.signal_state == "long":
            status = f"🟢 LONG, вход {self.entry_price:.2f}"
        elif self.signal_state == "short":
            status = f"🔴 SHORT, вход {self.entry_price:.2f}"
        else:
            status = "⚡ Нет сигнала"

        msg = (
            f"📊 <b>Текущий статус ETH/USDT</b>\n\n"
            f"Сигнал: {status}\n"
            f"Цена: {self.current_price:.2f}\n"
            f"RSI: {self.rsi:.1f}\n"
            f"ATR%: {self.atr_percent:.2f}%\n"
            f"Imbalance: {self.imbalance:.3f}\n"
            f"Funding: {self.funding_rate * 100:.4f}%\n"
            f"L/S Ratio: {self.long_short_ratio:.2f}\n"
            f"OI падение: {'✅' if self.oi_drop_detected else '❌'}\n"
            f"Торговля: {'✅' if self.is_trading_allowed() else '❌'}\n"
            f"Время: {now_local.strftime('%H:%M:%S')}"
        )
        await self.send_telegram(msg)

    async def _send_recent_signals(self) -> None:
        """Последние 5 сигналов из памяти"""
        signals = self.csv_logger.signals
        if not signals:
            await self.send_telegram("📋 Сигналов ещё не было.")
            return

        lines = ["📋 <b>Последние сигналы</b>\n"]
        for s in signals[-5:]:
            ts = s['timestamp'].strftime('%d.%m %H:%M')
            lines.append(f"{ts} — {s['signal_type']} @ {s['price']:.2f}")

        await self.send_telegram("\n".join(lines))

    async def send_signal(self, signal_type: str, price: float, reason: str) -> None:
        now_local = self.get_local_time()
        time_str = now_local.strftime("%Y-%m-%d %H:%M:%S")

        base_text = (
            f"Цена: {price:.2f}\n"
            f"Причина: {reason}\n"
            f"RSI: {self.rsi:.1f} | ATR%: {self.atr_percent:.2f}%\n"
            f"Время: {time_str}"
        )

        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "📊 TradingView", "url": "https://www.tradingview.com/chart/?symbol=BYBIT:ETHUSDT"},
                ],
                [
                    {"text": "📋 Статистика", "callback_data": "stats"},
                    {"text": "ℹ️ Help", "callback_data": "help"}
                ]
            ]
        }

        if signal_type == "LONG":
            self.signal_state = "long"
            self.entry_price = price
            self.entry_time = now_local
            message = f"🟢 <b>LONG СИГНАЛ</b>\n\n{base_text}"
            self.csv_logger.log_signal(
                signal_type="LONG", price=price, reason=reason,
                cvd=self.current_cvd, imbalance=self.imbalance,
                funding_rate=self.funding_rate, oi_drop=self.oi_drop_detected,
                rsi=self.rsi, atr_percent=self.atr_percent,
                long_short_ratio=self.long_short_ratio
            )

        elif signal_type == "SHORT":
            self.signal_state = "short"
            self.entry_price = price
            self.entry_time = now_local
            message = f"🔴 <b>SHORT СИГНАЛ</b>\n\n{base_text}"
            self.csv_logger.log_signal(
                signal_type="SHORT", price=price, reason=reason,
                cvd=self.current_cvd, imbalance=self.imbalance,
                funding_rate=self.funding_rate, oi_drop=self.oi_drop_detected,
                rsi=self.rsi, atr_percent=self.atr_percent,
                long_short_ratio=self.long_short_ratio
            )

        elif signal_type == "CLOSE_LONG":
            if self.entry_time:
                minutes = int((now_local - self.entry_time).total_seconds() / 60)
                duration_str = f"{minutes // 60}ч {minutes % 60}м" if minutes >= 60 else f"{minutes}м"
                pnl_pct = ((price - self.entry_price) / self.entry_price) * 100
                pnl_str = f"+{pnl_pct:.2f}%" if pnl_pct >= 0 else f"{pnl_pct:.2f}%"
            else:
                duration_str, pnl_str = "неизвестно", "N/A"

            message = (
                f"🟠 <b>CLOSE LONG</b>\n\n"
                f"Цена закрытия: {price:.2f}\n"
                f"Причина: {reason}\n"
                f"Время в позиции: {duration_str}\n"
                f"Симулированный PnL: {pnl_str}\n"
                f"Время: {time_str}"
            )
            self.signal_state = None
            self.entry_price = 0.0
            self.entry_time = None

        elif signal_type == "CLOSE_SHORT":
            if self.entry_time:
                minutes = int((now_local - self.entry_time).total_seconds() / 60)
                duration_str = f"{minutes // 60}ч {minutes % 60}м" if minutes >= 60 else f"{minutes}м"
                pnl_pct = ((self.entry_price - price) / self.entry_price) * 100
                pnl_str = f"+{pnl_pct:.2f}%" if pnl_pct >= 0 else f"{pnl_pct:.2f}%"
            else:
                duration_str, pnl_str = "неизвестно", "N/A"

            message = (
                f"🔵 <b>CLOSE SHORT</b>\n\n"
                f"Цена закрытия: {price:.2f}\n"
                f"Причина: {reason}\n"
                f"Время в позиции: {duration_str}\n"
                f"Симулированный PnL: {pnl_str}\n"
                f"Время: {time_str}"
            )
            self.signal_state = None
            self.entry_price = 0.0
            self.entry_time = None

        elif signal_type == "WARNING_SESSION_END":
            message = f"⏰ <b>ПРЕДУПРЕЖДЕНИЕ</b>\n\nСессия завершится через {reason}\nВремя: {time_str}"

        else:
            return

        await self.send_telegram(message, keyboard)
        logger.info(f"Сигнал отправлен: {signal_type}")

    async def handle_callback_query(self, callback_data: str) -> None:
        if callback_data == "stats":
            stats = self.csv_logger.get_statistics()
            if isinstance(stats, dict) and "message" in stats:
                await self.send_telegram(stats["message"])
            else:
                msg = (
                    f"📊 <b>Статистика</b>\n\n"
                    f"Всего: {stats.get('total_signals', 0)}\n"
                    f"LONG: {stats.get('long_signals', 0)}\n"
                    f"SHORT: {stats.get('short_signals', 0)}\n"
                    f"Последний: {stats.get('last_signal', 'нет')}\n\n"
                    f"RSI: {self.rsi:.1f} | ATR%: {self.atr_percent:.2f}%\n"
                    f"Imbalance: {self.imbalance:.3f}\n"
                    f"Funding: {self.funding_rate * 100:.4f}%"
                )
                await self.send_telegram(msg)

        elif callback_data == "help":
            await self.send_telegram(
                "🤖 <b>Справка</b>\n\n"
                "/ping — проверка\n"
                "/status — индикаторы\n"
                "/signals — последние сигналы\n"
                "/stats — статистика\n"
                "/help — это сообщение\n\n"
                "⚠️ Бот НЕ ТОРГУЕТ — только сигналы."
            )

    async def check_and_send_session_warnings(self) -> None:
        if not self.is_trading_allowed():
            return

        minutes_left = self.get_minutes_to_session_end()

        for threshold in [15, 10, 5, 1]:
            if minutes_left == threshold and self.last_warning_minute != threshold:
                await self.send_signal("WARNING_SESSION_END", self.current_price, f"{threshold} минут")
                self.last_warning_minute = threshold
                return

        if minutes_left <= 0:
            self.last_warning_minute = -1

    async def send_hourly_status(self) -> None:
        await self._send_status_message()

    # ========== HEALTHCHECK ==========

    async def healthcheck_handler(self, request):
        return web.Response(text="OK", status=200)

    async def start_healthcheck_server(self):
        self.web_app = web.Application()
        self.web_app.router.add_get('/health', self.healthcheck_handler)
        self.web_runner = web.AppRunner(self.web_app)
        await self.web_runner.setup()
        site = web.TCPSite(self.web_runner, '0.0.0.0', HEALTHCHECK_PORT)
        await site.start()
        logger.info(f"Healthcheck запущен на порту {HEALTHCHECK_PORT}")

    # ========== REST ==========

    async def fetch_open_interest(self) -> float:
        try:
            response = self.session.get_open_interest(
                category="linear", symbol="ETHUSDT", interval="5min"
            )
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
            logger.error(f"Funding rate fetch error: {e}")
        return 0.0

    # ИСПРАВЛЕНИЕ 1: правильный эндпоинт для Long/Short Ratio
    async def fetch_long_short_ratio(self) -> float:
        """
        Получает Long/Short Ratio через /v5/market/account-ratio.
        Возвращает долю лонгов (0.0–1.0), где 0.5 = паритет.
        """
        url = "https://api.bybit.com/v5/market/account-ratio"
        params = {
            "category": "linear",
            "symbol": "ETHUSDT",
            "period": "5min",
            "limit": 1,
        }
        try:
            async with ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status != 200:
                        return 0.5
                    data = await resp.json()

            if data.get("retCode") == 0:
                items = data.get("result", {}).get("list", [])
                if items:
                    buy_ratio = float(items[0].get("buyRatio", 0.5))
                    # buyRatio уже в диапазоне 0–1
                    return buy_ratio
        except Exception as e:
            logger.error(f"Long/Short ratio fetch error: {e}")
        return 0.5

    # ИСПРАВЛЕНИЕ 4: проверяем результаты сигналов через 1ч и 4ч
    async def check_signal_results(self) -> None:
        """Обновляет результаты сигналов в CSV через 1ч и 4ч после сигнала"""
        if self.current_price == 0:
            return

        now = datetime.now()
        for s in self.csv_logger.signals:
            age_hours = (now - s['timestamp']).total_seconds() / 3600

            if not s['checked_1h'] and age_hours >= 1.0:
                pct = ((self.current_price - s['price']) / s['price']) * 100
                if s['signal_type'] == 'SHORT':
                    pct = -pct
                self.csv_logger.update_result(s['ts_iso'], 1, pct)
                s['checked_1h'] = True

            if not s['checked_4h'] and age_hours >= 4.0:
                pct = ((self.current_price - s['price']) / s['price']) * 100
                if s['signal_type'] == 'SHORT':
                    pct = -pct
                self.csv_logger.update_result(s['ts_iso'], 4, pct)
                s['checked_4h'] = True

    async def update_rest_data(self) -> None:
        oi = await self.fetch_open_interest()
        if oi > 0:
            self.oi_history.append(oi)
            self._update_oi_drop()

        fr = await self.fetch_funding_rate()
        if fr != 0:
            self.funding_rate = fr

        self.long_short_ratio = await self.fetch_long_short_ratio()

    def _update_oi_drop(self) -> None:
        if len(self.oi_history) >= 3:
            curr_oi = self.oi_history[-1]
            prev_oi = self.oi_history[-3]
            if prev_oi > 0:
                oi_change = (curr_oi - prev_oi) / prev_oi
                self.oi_drop_detected = oi_change < OI_DROP_THRESHOLD

    # ========== ИНДИКАТОРЫ ==========

    def _update_cvd(self) -> None:
        if not self.trades:
            return

        delta = sum(t['size'] if t['side'] == 'Buy' else -t['size'] for t in self.trades)

        prev = self.cvd_history[-1] if self.cvd_history else 0
        self.cvd_history.append(prev + delta)
        self.current_cvd = self.cvd_history[-1]

        if len(self.cvd_history) >= 2:
            if self.cvd_history[-1] < self.cvd_history[-2]:
                self.cvd_down_count += 1
                self.cvd_up_count = 0
            elif self.cvd_history[-1] > self.cvd_history[-2]:
                self.cvd_up_count += 1
                self.cvd_down_count = 0
            else:
                self.cvd_down_count = 0
                self.cvd_up_count = 0

        self.trades.clear()

    def _update_imbalance(self) -> None:
        if not self.orderbook['bids'] or not self.orderbook['asks']:
            return

        bid_volume = sum(bid[1] for bid in self.orderbook['bids'][:IMBALANCE_LEVELS])
        ask_volume = sum(ask[1] for ask in self.orderbook['asks'][:IMBALANCE_LEVELS])

        total = bid_volume + ask_volume
        self.imbalance = bid_volume / total if total > 0 else 0.5

    # ========== СИГНАЛЫ ==========

    def _check_long_signal(self) -> Tuple[bool, str]:
        if self.signal_state is not None:
            return False, ""

        if self.atr_percent < MIN_ATR_PERCENT:
            return False, ""

        rsi_ok = self.rsi < RSI_LONG
        cvd_ok = self.cvd_down_count >= CVD_CONSECUTIVE
        imb_ok = self.imbalance < IMBALANCE_LONG
        fund_ok = self.funding_rate < FUNDING_LONG
        oi_ok = self.oi_drop_detected

        is_signal = cvd_ok and imb_ok and fund_ok and oi_ok

        if not is_signal:
            return False, ""

        conditions = []
        if cvd_ok:
            conditions.append(f"CVD↓ {self.cvd_down_count}")
        if imb_ok:
            conditions.append(f"Imb {self.imbalance:.3f}")
        if fund_ok:
            conditions.append(f"FR {self.funding_rate * 100:.4f}%")
        if oi_ok:
            conditions.append("OI↓")
        if rsi_ok:
            conditions.append(f"RSI {self.rsi:.1f}")

        return True, " | ".join(conditions)

    def _check_short_signal(self) -> Tuple[bool, str]:
        if self.signal_state is not None:
            return False, ""

        if self.atr_percent < MIN_ATR_PERCENT:
            return False, ""

        rsi_ok = self.rsi > RSI_SHORT
        cvd_ok = self.cvd_up_count >= CVD_CONSECUTIVE
        imb_ok = self.imbalance > IMBALANCE_SHORT
        fund_ok = self.funding_rate > FUNDING_SHORT
        oi_ok = self.oi_drop_detected

        is_signal = cvd_ok and imb_ok and fund_ok and oi_ok

        if not is_signal:
            return False, ""

        conditions = []
        if cvd_ok:
            conditions.append(f"CVD↑ {self.cvd_up_count}")
        if imb_ok:
            conditions.append(f"Imb {self.imbalance:.3f}")
        if fund_ok:
            conditions.append(f"FR {self.funding_rate * 100:.4f}%")
        if oi_ok:
            conditions.append("OI↓")
        if rsi_ok:
            conditions.append(f"RSI {self.rsi:.1f}")

        return True, " | ".join(conditions)

    def _check_close_long(self) -> Tuple[bool, str]:
        if self.signal_state != "long":
            return False, ""

        conditions = []
        trigger = False

        if self.cvd_down_count >= 2:
            trigger = True
            conditions.append(f"CVD↓ {self.cvd_down_count}")
        if self.imbalance > IMBALANCE_SHORT:
            trigger = True
            conditions.append(f"Imb {self.imbalance:.3f}")
        if self.funding_rate > FUNDING_CLOSE_LONG:
            trigger = True
            conditions.append(f"FR {self.funding_rate * 100:.4f}%")

        return trigger, " | ".join(conditions) if trigger else ""

    def _check_close_short(self) -> Tuple[bool, str]:
        if self.signal_state != "short":
            return False, ""

        conditions = []
        trigger = False

        if self.cvd_up_count >= 2:
            trigger = True
            conditions.append(f"CVD↑ {self.cvd_up_count}")
        if self.imbalance < IMBALANCE_LONG:
            trigger = True
            conditions.append(f"Imb {self.imbalance:.3f}")
        if self.funding_rate < FUNDING_CLOSE_SHORT:
            trigger = True
            conditions.append(f"FR {self.funding_rate * 100:.4f}%")

        return trigger, " | ".join(conditions) if trigger else ""

    # ========== WEBSOCKET ==========

    def _handle_trade(self, message: Dict) -> None:
        try:
            if "data" in message:
                for trade in message["data"]:
                    # Bybit v5: поля могут быть 'price'/'p', 'size'/'v', 'side'/'S'
                    price = float(trade.get('price') or trade.get('p') or 0)
                    size = float(trade.get('size') or trade.get('v') or 0)
                    side = trade.get('side') or trade.get('S') or 'Buy'
                    if price == 0:
                        continue
                    self.trades.append({
                        'price': price,
                        'size': size,
                        'side': side
                    })
                    self.current_price = price
                    self.price_history.append(price)
                    # ИСПРАВЛЕНИЕ 3: сохраняем (high, low, close) для ATR
                    # В trade-стриме нет H/L, используем price как H=L=C
                    # Полноценный ATR — в следующем шаге через kline-стрим
                    self.candle_history.append((price, price, price))
        except Exception as e:
            logger.error(f"Trade handler: {e}")

    def _handle_orderbook(self, message: Dict) -> None:
        try:
            if "data" in message:
                data = message["data"]
                if "bids" in data and "asks" in data:
                    self.orderbook['bids'] = [[float(b[0]), float(b[1])] for b in data['bids'][:10]]
                    self.orderbook['asks'] = [[float(a[0]), float(a[1])] for a in data['asks'][:10]]
                    self._update_imbalance()
        except Exception as e:
            logger.error(f"Orderbook handler: {e}")


    def _handle_kline(self, message: Dict) -> None:
        """Обработчик kline для правильного ATR (High/Low/Close)"""
        try:
            if "data" in message:
                for candle in message["data"]:
                    high = float(candle.get('high', 0))
                    low = float(candle.get('low', 0))
                    close = float(candle.get('close', 0))
                    if high > 0 and low > 0 and close > 0:
                        self.candle_history.append((high, low, close))
        except Exception as e:
            logger.error(f"Kline handler: {e}")

    async def _start_websockets(self) -> None:
        try:
            self.ws_trade = WebSocket(testnet=False, channel_type="public")
            self.ws_trade.trade_stream(symbol="ETHUSDT", callback=self._handle_trade)

            self.ws_orderbook = WebSocket(testnet=False, channel_type="public")
            self.ws_orderbook.orderbook_stream(symbol="ETHUSDT", depth=10, callback=self._handle_orderbook)


            # Kline для правильного ATR
            self.ws_kline = WebSocket(testnet=False, channel_type="public")
            self.ws_kline.kline_stream(interval=1, symbol="ETHUSDT", callback=self._handle_kline)

            logger.info("WebSocket подключения запущены")
        except Exception as e:
            logger.error(f"WebSocket startup error: {e}")
            raise

    # ========== ОСНОВНОЙ ЦИКЛ ==========

    async def _main_loop(self) -> None:
        while self.running:
            try:
                await self._run_cycle()
            except Exception as e:
                logger.critical(f"Критическая ошибка: {e}. Перезапуск через 60 сек...")
                await self.send_telegram(f"⚠️ <b>Критическая ошибка</b>\n{e}\nПерезапуск через 60 сек...")
                await asyncio.sleep(60)

    async def _run_cycle(self) -> None:
        while self.running:
            try:
                now = time.time()

                if not self.startup_sent:
                    await self.send_telegram(
                        f"✅ <b>Сигнальный робот ETH/USDT запущен</b>\n"
                        f"Часовой пояс: {TZ_LOCAL}\n"
                        f"Торговые часы: {TRADING_HOURS_START}:00–{TRADING_HOURS_END}:00\n"
                        f"Выходные: {'✅' if WEEKEND_TRADING else '❌'}\n"
                        f"ATR фильтр: {MIN_ATR_PERCENT}%\n"
                        f"RSI пороги: {RSI_LONG}/{RSI_SHORT}\n\n"
                        f"⚠️ Робот НЕ ТОРГУЕТ — только сигналы.\n\n"
                        f"Команды: /ping /status /signals /stats /help"
                    )
                    self.startup_sent = True

                # Polling Telegram-команд (ИСПРАВЛЕНИЕ 2)
                await self.poll_telegram_commands()

                await self.check_and_send_session_warnings()

                if now - self.last_rest_time >= REST_INTERVAL:
                    self.last_rest_time = now
                    await self.update_rest_data()

                self.update_ta_indicators()

                # Проверка результатов сигналов (ИСПРАВЛЕНИЕ 4)
                if now - self.last_result_check >= 300:  # каждые 5 минут
                    self.last_result_check = now
                    await self.check_signal_results()

                current_hour = int(now / 3600)
                if current_hour != self.last_hourly:
                    self.last_hourly = current_hour
                    await self.send_hourly_status()

                if not self.is_trading_allowed():
                    await asyncio.sleep(60)
                    continue

                if self.trades:
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
                    long_ok, long_reason = self._check_long_signal()
                    if long_ok:
                        await self.send_signal("LONG", self.current_price, long_reason)
                    else:
                        short_ok, short_reason = self._check_short_signal()
                        if short_ok:
                            await self.send_signal("SHORT", self.current_price, short_reason)

                await asyncio.sleep(UPDATE_INTERVAL)

            except Exception as e:
                logger.error(f"Cycle error: {e}")
                await asyncio.sleep(5)

    async def start(self) -> None:
        logger.info("Запуск сигнального робота ETH/USDT v2")
        await self.start_healthcheck_server()
        await self._start_websockets()
        await asyncio.sleep(3)
        await self._main_loop()

    def stop(self):
        self.running = False
        logger.info("Остановка бота...")


# ИСПРАВЛЕНИЕ 5: параметр переименован в sig, конфликт с модулем signal устранён
async def shutdown(sig, bot: EthSignalBot):
    logger.info(f"Получен сигнал {sig.name}")
    bot.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Бот остановлен")
    sys.exit(0)


async def main():
    bot = EthSignalBot()

    loop = asyncio.get_running_loop()
    # ИСПРАВЛЕНИЕ 5: используем signal_module вместо signal, sig вместо signal
    for sig in [signal_module.SIGINT, signal_module.SIGTERM]:
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s, bot))
        )

    try:
        await bot.start()
    except Exception as e:
        logger.critical(f"Не удалось запустить бота: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
