import os
import asyncio
import requests
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SYMBOL = "ETHUSDT"
UPDATE_INTERVAL = 60

last_signal = None


# =========================
# TELEGRAM
# =========================

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }

        response = requests.post(url, data=data)

        print("Telegram:", response.text)

    except Exception as e:
        print("Telegram error:", e)


# =========================
# BYBIT PRICE
# =========================

def get_eth_price():
    try:
        url = "https://api.bybit.com/v5/market/tickers"

        params = {
            "category": "linear",
            "symbol": SYMBOL
        }

        response = requests.get(url, params=params)

        data = response.json()

        price = float(
            data["result"]["list"][0]["lastPrice"]
        )

        return price

    except Exception as e:
        print("Bybit error:", e)
        return None


# =========================
# SIGNAL LOGIC
# =========================

def generate_signal(price):
    global last_signal

    if price is None:
        return

    if price > 4000 and last_signal != "SELL":
        last_signal = "SELL"

        send_telegram(
            f"🔴 SELL SIGNAL\n\nETH Price: {price}"
        )

    elif price < 3000 and last_signal != "BUY":
        last_signal = "BUY"

        send_telegram(
            f"🟢 BUY SIGNAL\n\nETH Price: {price}"
        )


# =========================
# MAIN LOOP
# =========================

async def trading_loop():
    while True:
        try:
            price = get_eth_price()

            print(f"ETH price: {price}")

            generate_signal(price)

        except Exception as e:
            print("Loop error:", e)

        await asyncio.sleep(UPDATE_INTERVAL)


# =========================
# WEB SERVER
# =========================

async def health(request):
    return web.Response(text="Bot is running")


async def start_web_server():
    app = web.Application()

    app.router.add_get("/", health)

    runner = web.AppRunner(app)

    await runner.setup()

    port = int(os.getenv("PORT", 8080))

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

    print(f"Web server started on port {port}")


# =========================
# MAIN
# =========================

async def main():
    print("Starting bot...")

    await start_web_server()

    send_telegram("🚀 ETH Signal Bot Started")

    await trading_loop()


if __name__ == "__main__":
    asyncio.run(main())
