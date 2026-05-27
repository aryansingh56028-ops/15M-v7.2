import ccxt.pro as ccxtpro  
import ccxt 
import pandas as pd
import pandas_ta as ta
import numpy as np
import asyncio
import aiohttp
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import os
import gc
import json
import pathlib
from datetime import datetime, timezone, date
from dotenv import load_dotenv

# Load Environment Variables from .env file
load_dotenv()

# ── 🔥 PRECISION SNIPER CRYPTO 24/7 (CLOUD-PROOF EDITION) 🔥 ──
market_data_cache = {}         
active_ws_tasks = {}       
active_watchlist = set()   
coin_tiers = {}            
PNL_FILE = 'daily_pnl.json'

_tg_semaphore = None
_math_semaphore = asyncio.Semaphore(1) 
processing_symbols = set()   

# ── Credentials & Config ───────────────────────────────────────────
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# 🛡️ Prop Firm Risk Management
DAILY_KILL_SWITCH  = -155.0   
EQUITY_HARD_STOP   = -120.0   
BASE_RISK_PER_TRADE = 25.0    
MAX_CONCURRENT     = 5        

# 📡 Radar & Watchlist
TREND_MIN_VOLUME       = 50000000   
RADAR_TOP_COINS        = 10        

# Custom Watchlist 
VIP_SYMBOLS = ['BTC/USDT:USDT', 'XRP/USDT:USDT', 'TRX/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT'] 

open_positions       = {}
daily_pnl_tracker    = {}

rest_exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET,
    'enableRateLimit': True, 'options': {'defaultType': 'swap'},
})
rest_exchange.enable_demo_trading(True) 

ws_exchange = ccxtpro.bybit({
    'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET, 
    'options': {'defaultType': 'swap'}
})
ws_exchange.enable_demo_trading(True)

# ── 🔥 STYLISH TERMINAL LOGS 🔥 ──
def stylish_log(action_type, symbol, message):
    now = datetime.now().strftime("%H:%M:%S")
    icons = {
        "SCANNING": "👀", "FOUND": "🎯", "SKIPPED": "⏭️",
        "EXECUTING": "⚡", "MANAGING": "🛡️", "CLOSED": "💰",
        "ERROR": "❌", "RADAR": "📡", "SYSTEM": "💻", "PROTECT": "🛑"
    }
    icon = icons.get(action_type, "🔹")
    action_padded = f"[{icon} {action_type}]".ljust(15)
    sym_padded = f"| {symbol.split(':')[0]} |".ljust(12) if symbol else "| BOT CORE |".ljust(12)
    print(f"[{now}] {action_padded} {sym_padded} {message}")

# ── 🔥 SYSTEM HELPERS 🔥 ──
def load_daily_pnl():
    try:
        data = json.loads(pathlib.Path(PNL_FILE).read_text())
        if data.get('date') == str(date.today()):
            daily_pnl_tracker[date.today()] = data['pnl']
            daily_pnl_tracker['equity_blown'] = data.get('equity_blown', False)
    except Exception: pass

def save_daily_pnl():
    try:
        pathlib.Path(PNL_FILE).write_text(json.dumps({
            'date': str(date.today()),
            'pnl': daily_pnl_tracker.get(date.today(), 0.0),
            'equity_blown': daily_pnl_tracker.get('equity_blown', False)
        }))
    except Exception: pass

async def send_telegram(text):
    global _tg_semaphore
    if _tg_semaphore is None: _tg_semaphore = asyncio.Semaphore(3)
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    
    async with _tg_semaphore:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=5) as response:
                    await response.text() 
        except Exception: pass

def is_kill_switch_active() -> bool:
    return daily_pnl_tracker.get('equity_blown', False) or daily_pnl_tracker.get(date.today(), 0.0) <= DAILY_KILL_SWITCH

# ── 🔥 BYBIT API HELPER 🔥 ──
async def update_bybit_sl(symbol, new_sl):
    """Forces Bybit to accept the trailing stop without silently failing."""
    try:
        f_sl = str(float(rest_exchange.price_to_precision(symbol, new_sl)))
        await asyncio.to_thread(
            rest_exchange.privatePostV5PositionTradingStop, 
            {
                'category': 'linear', 
                'symbol': symbol.split(':')[0], 
                'positionIdx': 0, 
                'stopLoss': f_sl,
                'tpslMode': 'Full'  # 🚨 Critical for Bybit V5 trailing logic
            }
        )
        return True
    except Exception as e:
        stylish_log("ERROR", symbol, f"Failed to modify SL on exchange: {e}")
        return False

# ── 🔥 PRECISION SNIPER ENGINE [CRYPTO 24/7] 🔥 ──
def calc_precision_sniper(df):
    df = df.copy()
    
    df.index = pd.to_datetime(df['ts'], unit='ms')
    df.sort_index(inplace=True) 
    
    df.ta.ema(length=9, append=True)
    df.ta.ema(length=21, append=True)
    df.ta.ema(length=55, append=True)
    df.ta.rsi(length=14, append=True)
    df.ta.atr(length=20, append=True)
    
    atr_col = [c for c in df.columns if c.startswith('ATR')][0]
    df.rename(columns={atr_col: 'ATR_20'}, inplace=True)
    
    df.ta.macd(fast=12, slow=26, signal=9, append=True)
    df.ta.vwap(append=True)
    df.ta.adx(length=14, append=True)
    
    df['VOL_SMA'] = df['volume'].rolling(20).mean()
    df['swing_low'] = df['low'].rolling(10).min().shift(1)
    df['swing_high'] = df['high'].rolling(10).max().shift(1)

    df_1h = df.resample('1h').agg({'close': 'last'}).dropna()
    df_1h.ta.ema(length=9, append=True)
    df_1h.ta.ema(length=21, append=True)
    df_1h['htf_bias'] = np.where(df_1h['EMA_9'] > df_1h['EMA_21'], 1, np.where(df_1h['EMA_9'] < df_1h['EMA_21'], -1, 0))
    df_1h['htf_bias'] = df_1h['htf_bias'].shift(1) 
    
    df = df.join(df_1h[['htf_bias']])
    df['htf_bias'] = df['htf_bias'].ffill().fillna(0)

    df['bull_score'] = 0.0
    df['bull_score'] += np.where(df['EMA_9'] > df['EMA_21'], 1.0, 0.0)
    df['bull_score'] += np.where(df['close'] > df['EMA_55'], 1.0, 0.0)
    df['bull_score'] += np.where((df['RSI_14'] > 50) & (df['RSI_14'] < 75), 1.0, 0.0)
    df['bull_score'] += np.where(df['MACDh_12_26_9'] > 0, 1.0, 0.0)
    df['bull_score'] += np.where(df['MACD_12_26_9'] > df['MACDs_12_26_9'], 1.0, 0.0)
    df['bull_score'] += np.where(df['close'] > df['VWAP_D'], 1.0, 0.0)
    df['bull_score'] += np.where(df['volume'] > (df['VOL_SMA'] * 1.2), 1.0, 0.0)
    df['bull_score'] += np.where((df['ADX_14'] > 20) & (df['DMP_14'] > df['DMN_14']), 1.0, 0.0)
    df['bull_score'] += np.where(df['htf_bias'] == 1, 1.5, 0.0)
    df['bull_score'] += np.where(df['close'] > df['EMA_9'], 0.5, 0.0)

    df['bear_score'] = 0.0
    df['bear_score'] += np.where(df['EMA_9'] < df['EMA_21'], 1.0, 0.0)
    df['bear_score'] += np.where(df['close'] < df['EMA_55'], 1.0, 0.0)
    df['bear_score'] += np.where((df['RSI_14'] < 50) & (df['RSI_14'] > 25), 1.0, 0.0)
    df['bear_score'] += np.where(df['MACDh_12_26_9'] < 0, 1.0, 0.0)
    df['bear_score'] += np.where(df['MACD_12_26_9'] < df['MACDs_12_26_9'], 1.0, 0.0)
    df['bear_score'] += np.where(df['close'] < df['VWAP_D'], 1.0, 0.0)
    df['bear_score'] += np.where(df['volume'] > (df['VOL_SMA'] * 1.2), 1.0, 0.0)
    df['bear_score'] += np.where((df['ADX_14'] > 20) & (df['DMN_14'] > df['DMP_14']), 1.0, 0.0)
    df['bear_score'] += np.where(df['htf_bias'] == -1, 1.5, 0.0)
    df['bear_score'] += np.where(df['close'] < df['EMA_9'], 0.5, 0.0)

    df['ema_bull_cross'] = (df['EMA_9'] > df['EMA_21']) & (df['EMA_9'].shift(1) <= df['EMA_21'].shift(1))
    df['ema_bear_cross'] = (df['EMA_9'] < df['EMA_21']) & (df['EMA_9'].shift(1) >= df['EMA_21'].shift(1))

    df['buy_signal'] = df['ema_bull_cross'] & (df['close'] > df['EMA_9']) & (df['close'] > df['EMA_21']) & (df['RSI_14'] < 75) & (df['bull_score'] >= 5.0)
    df['sell_signal'] = df['ema_bear_cross'] & (df['close'] < df['EMA_9']) & (df['close'] < df['EMA_21']) & (df['RSI_14'] > 25) & (df['bear_score'] >= 5.0)

    return df

def seed_historical_data(symbol, tf):
    try:
        bars = rest_exchange.fetch_ohlcv(symbol, tf, limit=1000)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        for c in ['open', 'high', 'low', 'close']: df[c] = df[c].astype(float)
        market_data_cache[f"{symbol}_{tf}"] = df
    except Exception: pass

# ── 🔥 EXECUTION 🔥 ──
async def execute_trade_market(symbol, direction, risk_usd, df_row):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        trigger_px = float(df_row['close'])
        atr_val = float(df_row['ATR_20'])
        
        atr_risk = atr_val * 2.0
        atr_sl = trigger_px - atr_risk if direction == 'LONG' else trigger_px + atr_risk
        
        struct_stop = float(df_row['swing_low']) - (atr_val * 0.2) if direction == 'LONG' else float(df_row['swing_high']) + (atr_val * 0.2)
        final_sl = max(atr_sl, struct_stop) if direction == 'LONG' else min(atr_sl, struct_stop)
        
        sl_dist = abs(trigger_px - final_sl)
        
        tp1_px = trigger_px + (sl_dist * 1.0) if direction == 'LONG' else trigger_px - (sl_dist * 1.0)
        tp2_px = trigger_px + (sl_dist * 2.0) if direction == 'LONG' else trigger_px - (sl_dist * 2.0)
        tp3_px = trigger_px + (sl_dist * 3.0) if direction == 'LONG' else trigger_px - (sl_dist * 3.0)

        sz = risk_usd / sl_dist
        f_sz = float(rest_exchange.amount_to_precision(symbol, sz))
        f_sl = str(float(rest_exchange.price_to_precision(symbol, final_sl)))
        
        # 🚨 FIX: Force physical TP3 onto the exchange for safety
        f_tp3 = str(float(rest_exchange.price_to_precision(symbol, tp3_px)))
        
        stylish_log("EXECUTING", symbol, f"{direction} Triggered. Sizing: {f_sz} | SL: {f_sl}")
        
        await asyncio.to_thread(
            rest_exchange.create_order,
            symbol=symbol, type='market', side=side, amount=f_sz, 
            params={'stopLoss': f_sl, 'takeProfit': f_tp3, 'positionIdx': 0, 'tpslMode': 'Full'} 
        )
            
        open_positions[symbol] = {
            'direction': direction, 'entry': trigger_px, 'qty': f_sz,
            'sl_dist': sl_dist,
            'sl': final_sl, 'trail_price': final_sl,
            'tp1': tp1_px, 'tp2': tp2_px, 'tp3': tp3_px,
            'tp1_hit': False, 'tp2_hit': False
        }

        icon = "🟢" if direction == "LONG" else "🔴"
        await send_telegram(f"{icon} <b>SNIPER ENTRY: {symbol}</b>\nDirection: {direction}\nEntry: {trigger_px:.4f}\nSL: {final_sl:.4f}\nTP1 (1R): {tp1_px:.4f}\nTP3 (3R): {tp3_px:.4f}")

    except Exception as e: stylish_log("ERROR", symbol, f"Execution failed: {e}")

async def analyze_structure(symbol):
    if symbol in processing_symbols or symbol in open_positions or is_kill_switch_active(): return
    if len(open_positions) >= MAX_CONCURRENT: return
    
    processing_symbols.add(symbol)
    try:
        df_5m = market_data_cache.get(f"{symbol}_5m")
        if df_5m is None or len(df_5m) < 850: return
        
        global _math_semaphore
        async with _math_semaphore:
            df = await asyncio.to_thread(calc_precision_sniper, df_5m)
            
        c_bar = df.iloc[-2] 
        
        if c_bar['buy_signal']:
            stylish_log("FOUND", symbol, f"LONG Setup Confirmed. Score: {c_bar['bull_score']}/10")
            await execute_trade_market(symbol, "LONG", BASE_RISK_PER_TRADE, c_bar)
        elif c_bar['sell_signal']:
            stylish_log("FOUND", symbol, f"SHORT Setup Confirmed. Score: {c_bar['bear_score']}/10")
            await execute_trade_market(symbol, "SHORT", BASE_RISK_PER_TRADE, c_bar)
    finally: processing_symbols.discard(symbol)

# ── 🔥 ASYNC WEBSOCKET ENGINE (TICK-SPEED MANAGER) 🔥 ──
async def watch_ticker_stream(exchange, symbol):
    last_seen_ts = 0
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(symbol, '5m')
            new_bar = ohlcv[0]
            bar_ts = int(new_bar[0])
            cur_px = float(new_bar[4]) 
            
            if symbol in open_positions:
                pos = open_positions[symbol]
                is_l = pos['direction'] == 'LONG'
                
                # 1. TP3 Full Target Hit (Or Exchange closed it physically)
                if (is_l and cur_px >= pos['tp3']) or (not is_l and cur_px <= pos['tp3']):
                    stylish_log("CLOSED", symbol, "TP3 Target Reached! Securing full 3R profit.")
                    
                    captured_r = (cur_px - pos['entry']) / pos['sl_dist'] if is_l else (pos['entry'] - cur_px) / pos['sl_dist']
                    trade_pnl_usd = captured_r * BASE_RISK_PER_TRADE
                    daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + trade_pnl_usd
                    save_daily_pnl()
                    
                    msg = f"🏆 <b>TP3 FULL TARGET: {symbol}</b>\nDirection: {pos['direction']}\nExit Price: {cur_px:.4f}\nCaptured: +{captured_r:.2f}R (~${trade_pnl_usd:.2f})"
                    asyncio.create_task(send_telegram(msg))
                    open_positions.pop(symbol)
                    
                # 2. Ratchet to TP1
                elif not pos.get('tp2_hit') and ((is_l and cur_px >= pos['tp2']) or (not is_l and cur_px <= pos['tp2'])):
                    stylish_log("MANAGING", symbol, "Price hit TP2. Trailing Stop Loss to TP1.")
                    pos['tp2_hit'] = True
                    pos['trail_price'] = pos['tp1']
                    
                    asyncio.create_task(update_bybit_sl(symbol, pos['tp1']))
                    
                    msg = f"🛡️ <b>TP2 HIT: {symbol}</b>\nDirection: {pos['direction']}\nStatus: SL trailed to TP1 (+1.00R Locked)"
                    asyncio.create_task(send_telegram(msg))

                # 3. Ratchet to Breakeven
                elif not pos.get('tp1_hit') and ((is_l and cur_px >= pos['tp1']) or (not is_l and cur_px <= pos['tp1'])):
                    stylish_log("MANAGING", symbol, "Price hit TP1. Trailing Stop Loss to Breakeven + Fees.")
                    pos['tp1_hit'] = True
                    be_px = pos['entry'] * 1.0015 if is_l else pos['entry'] * 0.9985
                    pos['trail_price'] = be_px
                    
                    asyncio.create_task(update_bybit_sl(symbol, be_px))
                    
                    msg = f"🎯 <b>TP1 HIT: {symbol}</b>\nDirection: {pos['direction']}\nStatus: Breakeven Activated (Risk-Free)"
                    asyncio.create_task(send_telegram(msg))
                
                # 4. SL or Trail Hit
                elif (is_l and cur_px <= pos['trail_price']) or (not is_l and cur_px >= pos['trail_price']):
                    stylish_log("CLOSED", symbol, "Stop Loss / Trail triggered on websocket.")
                    
                    captured_r = (pos['trail_price'] - pos['entry']) / pos['sl_dist'] if is_l else (pos['entry'] - pos['trail_price']) / pos['sl_dist']
                    trade_pnl_usd = captured_r * BASE_RISK_PER_TRADE
                    daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + trade_pnl_usd
                    save_daily_pnl()
                    
                    icon = "🛑" if captured_r <= 0 else "🛡️"
                    msg = f"{icon} <b>SL / TRAIL HIT: {symbol}</b>\nDirection: {pos['direction']}\nExit Price: {pos['trail_price']:.4f}\nCaptured: {captured_r:.2f}R (~${trade_pnl_usd:.2f})"
                    asyncio.create_task(send_telegram(msg))
                    open_positions.pop(symbol)

            df = market_data_cache.get(f"{symbol}_5m")
            if df is not None:
                if bar_ts > last_seen_ts:
                    if last_seen_ts > 0: 
                        new_row = pd.DataFrame([[new_bar[0], new_bar[1], new_bar[2], new_bar[3], cur_px, new_bar[5]]], columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
                        df = pd.concat([df, new_row], ignore_index=True)
                        df = df.tail(1000).reset_index(drop=True)
                        market_data_cache[f"{symbol}_5m"] = df
                        await analyze_structure(symbol)
                    else:
                        df.iloc[-1] = [new_bar[0], new_bar[1], new_bar[2], new_bar[3], cur_px, new_bar[5]]
                        market_data_cache[f"{symbol}_5m"] = df
                    last_seen_ts = bar_ts
                else:
                    df.iloc[-1] = [new_bar[0], new_bar[1], new_bar[2], new_bar[3], cur_px, new_bar[5]]
                    market_data_cache[f"{symbol}_5m"] = df
                    
        except asyncio.CancelledError: break 
        except Exception: await asyncio.sleep(2)

# ── 🔥 EQUITY PROTECTION LOOP 🔥 ──
async def equity_protection_loop():
    while True:
        try:
            if daily_pnl_tracker.get('equity_blown', False):
                await asyncio.sleep(60)
                continue
                
            pos_data = await asyncio.to_thread(rest_exchange.fetch_positions)
            unrealized_pnl = sum(float(p.get('unrealisedPnl', 0.0)) for p in pos_data if float(p.get('contracts', 0)) > 0)
            realized_pnl = daily_pnl_tracker.get(date.today(), 0.0)
            live_equity = realized_pnl + unrealized_pnl
            
            if live_equity <= EQUITY_HARD_STOP:
                daily_pnl_tracker['equity_blown'] = True
                save_daily_pnl()
                stylish_log("PROTECT", None, f"Live equity ({live_equity:.2f}) breached hard stop. Halting.")
                await send_telegram(f"🚨 <b>EQUITY CIRCUIT BREAKER</b> 🚨\nTotal Equity: {live_equity:.2f}\nTrading halted.")
                
            live_syms = [p['symbol'] for p in pos_data if float(p.get('contracts', 0)) > 0]
            for sym in list(open_positions.keys()):
                base = sym.split(':')[0]
                if base not in live_syms:
                    pos = open_positions[sym]
                    stylish_log("CLOSED", sym, "Position closed externally by exchange (SL Hit).")
                    
                    is_l = pos['direction'] == 'LONG'
                    exit_px = pos['trail_price'] 
                    
                    captured_r = (exit_px - pos['entry']) / pos['sl_dist'] if is_l else (pos['entry'] - exit_px) / pos['sl_dist']
                    trade_pnl_usd = captured_r * BASE_RISK_PER_TRADE
                    
                    daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + trade_pnl_usd
                    save_daily_pnl()
                    
                    icon = "🛑" if captured_r <= 0 else "🛡️"
                    msg = f"{icon} <b>EXCHANGE CLOSE: {sym}</b>\nDirection: {pos['direction']}\nApprox Exit: {exit_px:.4f}\nCaptured: {captured_r:.2f}R (~${trade_pnl_usd:.2f})"
                    asyncio.create_task(send_telegram(msg))
                    
                    open_positions.pop(sym, None)
                    
        except Exception: pass
        await asyncio.sleep(10)

# ── 🔥 DYNAMIC RADAR 🔥 ──
async def dynamic_radar_loop():
    for sym in VIP_SYMBOLS: coin_tiers[sym] = {'tier': 1}
    while True:
        stylish_log("SCANNING", None, "Sweeping markets for volume expansion...")
        try:
            tickers = await asyncio.to_thread(rest_exchange.fetch_tickers)
            candidates = [{'symbol': s, 'vol': float(d.get('quoteVolume', 0))} for s, d in tickers.items() if s.endswith(':USDT') and float(d.get('quoteVolume', 0)) >= TREND_MIN_VOLUME]
            
            candidates.sort(key=lambda x: x['vol'], reverse=True)
            new_watchlist = set(VIP_SYMBOLS + [c['symbol'] for c in candidates[:RADAR_TOP_COINS]])
            global active_watchlist
            
            for sym in list(active_watchlist):
                if sym not in new_watchlist and sym not in open_positions and sym in active_ws_tasks:
                    active_ws_tasks[sym].cancel()
                    active_ws_tasks.pop(sym, None)
                    
            for sym in new_watchlist:
                if sym not in active_ws_tasks:
                    await asyncio.to_thread(seed_historical_data, sym, '5m') 
                    task = asyncio.create_task(watch_ticker_stream(ws_exchange, sym))
                    active_ws_tasks[sym] = task
                    await asyncio.sleep(1.5)
                    
            active_watchlist = new_watchlist
            stylish_log("RADAR", None, f"Sweep complete. Watching {len(active_watchlist)} assets.")
            await asyncio.to_thread(gc.collect)
        except Exception: pass
        await asyncio.sleep(1800)

# ── 🔥 THREADED KEEPALIVE SERVER 🔥 ──
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"200 OK - ALIVE")

    def log_message(self, format, *args):
        pass 

def start_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    stylish_log("SYSTEM", None, f"Threaded health server active on port {port}.")
    server.serve_forever()

# ── 🔥 BOOT SEQUENCE 🔥 ──
async def main():
    print("======================================================")
    print("  🎯 PRECISION SNIPER CRYPTO 24/7 (CLOUD-PROOF EDITION)")
    print("======================================================\n")
    load_daily_pnl()
    stylish_log("SYSTEM", None, "Booting Precision Engine & Async Websockets...")
    
    threading.Thread(target=start_health_server, daemon=True).start()
    
    await send_telegram(f"🎯 <b>Precision Sniper ONLINE</b>\nTick-Speed Manager Active.")
    
    try:
        await asyncio.gather(dynamic_radar_loop(), equity_protection_loop())
    except Exception as e:
        stylish_log("ERROR", None, f"Fatal loop crash: {e}")

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
