"""
GOAT 5m Scalper - Prop Firm Edition (Demo Account)
Elite 10 Symbols | Dual-TF MACD | Limit Order Execution
Stateless Kill Switch | Concurrency Limit | Dynamic Balance
Optimised: TP=7.5×ATR | Risk=$35/trade (backtest: 49 funded / 52.0% P1 pass rate)

BUG FIX: tpslMode + slOrderType + tpOrderType added to prevent blank SL/TP on Bybit.
"""

import ccxt
import pandas as pd
import numpy as np
import requests
import schedule
import time

# ── Fill these in ──────────────────────────────────────────────────────────────
BYBIT_API_KEY      = "4ivdCMqNAwAEJSmnAU"
BYBIT_API_SECRET   = "gmWWR8BQK65NlrzAfsD6DhXLU5wg75QpNiTq"
TELEGRAM_BOT_TOKEN = "8586984642:AAEMFum2ICKmwS1NF8XYmUNDxRdYN7aRJmY"
TELEGRAM_CHAT_ID   = "1932328527"
# ──────────────────────────────────────────────────────────────────────────────

# ── Risk Parameters (matches backtest exactly) ─────────────────────────────────
RISK_PER_TRADE_USD    = 35.0     # $35 fixed risk per trade
ATR_SL_MULT           = 1.5      # SL = 1.5 × ATR below/above entry
ATR_TP_MULT           = 7.5      # TP = 7.5 × ATR above/below entry  ← UPDATED
DAILY_LOSS_LIMIT      = -75.0    # kill switch threshold
MAX_CONCURRENT_TRADES = 3        # max simultaneous open positions

SYMBOLS = [
    'BTC/USDT:USDT', 'SOL/USDT:USDT', 'ONDO/USDT:USDT', 'AVAX/USDT:USDT',
    'ZEC/USDT:USDT', 'INJ/USDT:USDT', 'LTC/USDT:USDT', 'ADA/USDT:USDT',
    'NEAR/USDT:USDT', 'LINK/USDT:USDT'
]
TIMEFRAMES = {'macro': '1h', 'exec': '5m'}

# ── Exchange (Demo) ────────────────────────────────────────────────────────────
exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY,
    'secret': BYBIT_API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'enableDemoTrading': True,
    },
    'headers': {
        'X-BAPI-DEMO': '1'
    }
})
exchange.urls['api']['public']  = 'https://api-demo.bybit.com'
exchange.urls['api']['private'] = 'https://api-demo.bybit.com'
exchange.urls['api']['v5']      = 'https://api-demo.bybit.com'
exchange.load_markets()

# ── Telegram ───────────────────────────────────────────────────────────────────

def send_telegram(message):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message.strip(), "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

def send_startup_message():
    balance = get_account_balance()
    msg = (
        f"🚀 *GOAT 5m Scalper (Demo — Optimised)* 🚀\n"
        f"💰 *Balance:* ${balance:,.2f}\n"
        f"⚙️ *Mode:* Demo | Limit Orders | 20x Cross\n"
        f"🎯 *TP:* 7.5×ATR | *SL:* 1.5×ATR | *RR:* 1:5\n"
        f"🛡️ *Max Open Trades:* {MAX_CONCURRENT_TRADES}\n"
        f"⚠️ *Fixed Risk:* ${RISK_PER_TRADE_USD:.2f}/trade\n"
        f"🛑 *Daily Kill Switch:* ${DAILY_LOSS_LIMIT:.2f}\n"
        f"📊 *Backtest:* 49 funded / 52.0% P1 pass rate"
    )
    send_telegram(msg)

# ── Indicators ─────────────────────────────────────────────────────────────────

def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def calc_atr(high, low, close, length=14):
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(span=length, adjust=False).mean()

def calc_macd(close, fast=12, slow=26, signal=9):
    macd_line = ema(close, fast) - ema(close, slow)
    sig_line  = ema(macd_line, signal)
    return macd_line, sig_line

def fetch_data(symbol, timeframe, limit=300):
    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df   = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        print(f"  [fetch_data] {symbol} {timeframe}: {e}", flush=True)
        return None

# ── Account ────────────────────────────────────────────────────────────────────

def get_account_balance():
    try:
        info = exchange.fetch_balance()
        return float(info.get('USDT', {}).get('free', 5000.0))
    except Exception as e:
        print(f"  [balance] fetch failed: {e} — defaulting to 5000", flush=True)
        return 5000.0

def calculate_position_size(entry_price, stop_loss_price):
    distance = abs(entry_price - stop_loss_price)
    if distance == 0:
        return 0
    return RISK_PER_TRADE_USD / distance

# ── Stateless Kill Switch ──────────────────────────────────────────────────────

def get_today_realized_pnl():
    try:
        midnight_utc = int(
            pd.Timestamp.now('UTC')
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp() * 1000
        )
        daily_pnl = 0.0
        for symbol in SYMBOLS:
            clean_symbol = symbol.split(':')[0].replace('/', '')
            res = exchange.privateGetV5PositionClosedPnl({
                'category': 'linear',
                'symbol':   clean_symbol,
                'startTime': midnight_utc,
            })
            if 'result' in res and 'list' in res['result']:
                for item in res['result']['list']:
                    daily_pnl += float(item.get('closedPnl', 0))
        return daily_pnl
    except Exception as e:
        print(f"  [kill switch] PnL fetch error: {e}", flush=True)
        return 0.0

# ── Leverage / Margin Setup ────────────────────────────────────────────────────

def set_leverage_and_margin():
    print("⚙️  Setting cross margin & 20x leverage...", flush=True)
    for symbol in SYMBOLS:
        try:
            exchange.set_margin_mode('cross', symbol)
        except Exception:
            pass
        try:
            exchange.set_leverage(20, symbol)
        except Exception:
            pass

# ── Order Helper (fixes the blank SL/TP bug) ──────────────────────────────────

def place_limit_order(symbol, side, size, fmt_entry, fmt_sl, fmt_tp):
    return exchange.create_order(
        symbol=symbol,
        type='limit',
        side=side,
        amount=size,
        price=fmt_entry,
        params={
            'stopLoss':    fmt_sl,
            'takeProfit':  fmt_tp,
            'tpslMode':    'Full',
            'slOrderType': 'Market',
            'tpOrderType': 'Limit',
        }
    )

# ── Main Scan ──────────────────────────────────────────────────────────────────

def analyze_market():
    ts = pd.Timestamp.now('UTC').strftime('%H:%M:%S')
    print(f"[{ts}] 👀 Scanning pairs...", flush=True)

    # 1. STATELESS KILL SWITCH
    daily_pnl = get_today_realized_pnl()
    if daily_pnl <= DAILY_LOSS_LIMIT:
        print(f"  🛑 KILL SWITCH: daily PnL ${daily_pnl:.2f}. No new trades today.", flush=True)
        return

    # 2. GLOBAL CONCURRENCY CHECK
    active_trades = 0
    try:
        all_positions = exchange.fetch_positions()
        for pos in all_positions:
            if float(pos.get('contracts', 0)) > 0:
                active_trades += 1
    except Exception as e:
        print(f"  [concurrency] position fetch error: {e}", flush=True)

    if active_trades >= MAX_CONCURRENT_TRADES:
        print(f"  🔒 Concurrency limit: {active_trades}/{MAX_CONCURRENT_TRADES} open. Skipping scan.", flush=True)
        return

    for symbol in SYMBOLS:

        # 3. CANCEL STALE UNFILLED LIMIT ORDERS
        try:
            exchange.cancel_all_orders(symbol)
        except Exception:
            pass

        # 4. SKIP IF ALREADY HAVE A POSITION ON THIS SYMBOL
        try:
            positions = exchange.fetch_positions([symbol])
            if any(float(p.get('contracts', 0)) > 0 for p in positions):
                continue
        except Exception:
            continue

        # 5. FETCH DATA (600 bars on 1H for EMA 200 warmup)
        df_1h = fetch_data(symbol, TIMEFRAMES['macro'], limit=600)
        df_5m = fetch_data(symbol, TIMEFRAMES['exec'],  limit=300)
        if df_1h is None or df_5m is None or len(df_1h) < 210 or len(df_5m) < 50:
            continue

        # 6. INDICATORS
        df_1h['EMA_200'] = ema(df_1h['close'], 200)
        df_1h['EMA_50']  = ema(df_1h['close'], 50)
        df_1h['MACD'], df_1h['MACD_Signal'] = calc_macd(df_1h['close'])

        df_5m['EMA_21'] = ema(df_5m['close'], 21)
        df_5m['MACD'], df_5m['MACD_Signal'] = calc_macd(df_5m['close'])
        df_5m['ATR']    = calc_atr(df_5m['high'], df_5m['low'], df_5m['close'], 14)

        c1h = df_1h.iloc[-2]   # last confirmed closed 1H bar
        c5m = df_5m.iloc[-2]   # last confirmed closed 5m bar
        p5m = df_5m.iloc[-3]   # previous 5m bar (MACD cross detection)

        entry_price = c5m['close']
        atr_value   = c5m['ATR']
        if atr_value <= 0:
            continue

        # 7. SIGNAL CONDITIONS (identical to backtest logic)
        bullish_macro      = c1h['close'] > c1h['EMA_200'] and c1h['close'] > c1h['EMA_50']
        bearish_macro      = c1h['close'] < c1h['EMA_200'] and c1h['close'] < c1h['EMA_50']
        macro_macd_bullish = c1h['MACD'] > c1h['MACD_Signal']
        macro_macd_bearish = c1h['MACD'] < c1h['MACD_Signal']
        macd_cross_up      = p5m['MACD'] < p5m['MACD_Signal'] and c5m['MACD'] > c5m['MACD_Signal']
        macd_cross_down    = p5m['MACD'] > p5m['MACD_Signal'] and c5m['MACD'] < c5m['MACD_Signal']
        ema21_touch_long   = c5m['low']  <= c5m['EMA_21']
        ema21_touch_short  = c5m['high'] >= c5m['EMA_21']

        # 8. LONG SIGNAL
        if bullish_macro and macro_macd_bullish and ema21_touch_long and macd_cross_up:
            sl  = entry_price - ATR_SL_MULT * atr_value
            tp  = entry_price + ATR_TP_MULT * atr_value
            raw_size = calculate_position_size(entry_price, sl)
            try:
                size      = float(exchange.amount_to_precision(symbol, raw_size))
                fmt_entry = float(exchange.price_to_precision(symbol, entry_price))
                fmt_sl    = float(exchange.price_to_precision(symbol, sl))
                fmt_tp    = float(exchange.price_to_precision(symbol, tp))

                place_limit_order(symbol, 'buy', size, fmt_entry, fmt_sl, fmt_tp)

                msg = (
                    f"⚡ *NEW LIMIT TRADE* ⚡\n"
                    f"🟢 LONG {symbol.split(':')[0]}\n"
                    f"Entry: `{fmt_entry}` | SL: `{fmt_sl}` | TP: `{fmt_tp}`\n"
                    f"Size: `{size}` | Risk: `${RISK_PER_TRADE_USD:.2f}` | RR: 1:5"
                )
                send_telegram(msg)
                print(f"  ✅ LONG {symbol} @ {fmt_entry}  SL={fmt_sl}  TP={fmt_tp}", flush=True)

                active_trades += 1
                if active_trades >= MAX_CONCURRENT_TRADES:
                    break

            except Exception as e:
                err = f"❌ Order FAILED: LONG {symbol.split(':')[0]} — `{e}`"
                print(f"  {err}", flush=True)
                send_telegram(err)

        # 9. SHORT SIGNAL
        elif bearish_macro and macro_macd_bearish and ema21_touch_short and macd_cross_down:
            sl  = entry_price + ATR_SL_MULT * atr_value
            tp  = entry_price - ATR_TP_MULT * atr_value
            raw_size = calculate_position_size(entry_price, sl)
            try:
                size      = float(exchange.amount_to_precision(symbol, raw_size))
                fmt_entry = float(exchange.price_to_precision(symbol, entry_price))
                fmt_sl    = float(exchange.price_to_precision(symbol, sl))
                fmt_tp    = float(exchange.price_to_precision(symbol, tp))

                place_limit_order(symbol, 'sell', size, fmt_entry, fmt_sl, fmt_tp)

                msg = (
                    f"⚡ *NEW LIMIT TRADE* ⚡\n"
                    f"🔴 SHORT {symbol.split(':')[0]}\n"
                    f"Entry: `{fmt_entry}` | SL: `{fmt_sl}` | TP: `{fmt_tp}`\n"
                    f"Size: `{size}` | Risk: `${RISK_PER_TRADE_USD:.2f}` | RR: 1:5"
                )
                send_telegram(msg)
                print(f"  ✅ SHORT {symbol} @ {fmt_entry}  SL={fmt_sl}  TP={fmt_tp}", flush=True)

                active_trades += 1
                if active_trades >= MAX_CONCURRENT_TRADES:
                    break

            except Exception as e:
                err = f"❌ Order FAILED: SHORT {symbol.split(':')[0]} — `{e}`"
                print(f"  {err}", flush=True)
                send_telegram(err)

# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    set_leverage_and_margin()
    send_startup_message()
    analyze_market()
    schedule.every(5).minutes.at(":00").do(analyze_market)
    while True:
        schedule.run_pending()
        time.sleep(1)
