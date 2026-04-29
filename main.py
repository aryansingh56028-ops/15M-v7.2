import ccxt
import pandas as pd
import numpy as np
import requests
import schedule
import time
import math
import json
import threading
import os
from datetime import datetime, timezone, date

# ── Credentials & Config ───────────────────────────────────────────
BYBIT_API_KEY    = "FOqGNCN6gRxu4bqMqF"      
BYBIT_API_SECRET = "YmSWYNkQbVXYiFU5v0G3y3R405VLREGu7icy"   
TELEGRAM_BOT_TOKEN = "8734785957:AAGzU-KPRY4mzXARxyTpLSHGemFtJ7AEsUQ"  
TELEGRAM_CHAT_ID   = "1932328527"               

CURRENT_PHASE     = 1        
DAILY_KILL_SWITCH = -180.0   
MAX_CONCURRENT    = 5        
FEE_CAP_FRAC      = 0.40     

# 🔥 HOUSE MONEY & RADAR CONFIG
HOUSE_MONEY_THRESHOLD  = 60.0  
HOUSE_MONEY_MULTIPLIER = 1.5   
RADAR_MIN_VOLUME       = 75000000  
RADAR_TOP_COINS        = 15        
P1_RISK = 25.0                     
P2_RISK = 25.0
STATE_FILE = "bot_state.json"

# ── Runtime State ──────────────────────────────────────────────────
open_positions       = {}
pending_orders       = {}  
daily_pnl_tracker    = {}
last_trade_bar       = {}  
active_watchlist     = []
edge_cooldowns       = {}  
approved_coins       = {}  
is_scanning          = False

# ── Exchange & Persistence ─────────────────────────────────────────
exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY,
    'secret': BYBIT_API_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})
exchange.enable_demo_trading(True) 
exchange.load_markets()

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump({'open': open_positions, 'pending': pending_orders}, f)

def load_state():
    global open_positions, pending_orders
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            open_positions = data.get('open', {})
            pending_orders = data.get('pending', {})

# ── Telegram ───────────────────────────────────────────────────────
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try: requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'}, timeout=10)
    except Exception: pass

def is_kill_switch_active() -> bool:
    return daily_pnl_tracker.get(date.today(), 0.0) <= DAILY_KILL_SWITCH

def record_closed_pnl(pnl_usd: float):
    today = date.today()
    daily_pnl_tracker[today] = daily_pnl_tracker.get(today, 0.0) + pnl_usd

# ── 🧠 INSTITUTIONAL TOOLKIT & DATA ────────────────────────────────
def fetch_deep_data(symbol, timeframe='15m', target_limit=3000):
    try:
        since = exchange.milliseconds() - (target_limit * 15 * 60 * 1000)
        all_ohlcv = []
        while len(all_ohlcv) < target_limit:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
            if not ohlcv: break
            since = ohlcv[-1][0] + 1
            all_ohlcv.extend(ohlcv)
            time.sleep(0.1)
        df = pd.DataFrame(all_ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        for c in ['open', 'high', 'low', 'close']: df[c] = df[c].astype(float)
        return df
    except Exception: return None

def add_vwap(df):
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    df['date'] = df['datetime'].dt.date
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['pv'] = df['typical_price'] * df['volume']
    df['daily_vwap'] = df.groupby('date')['pv'].cumsum() / df.groupby('date')['volume'].cumsum()
    return df

def add_fvg_obv(df):
    df['fvg_bull'] = df['low'] > df['high'].shift(2)
    df['fvg_bear'] = df['high'] < df['low'].shift(2)
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
    df['obv_ema'] = df['obv'].ewm(span=20, adjust=False).mean()
    return df

def add_squeeze(df, length=20):
    df['basis'] = df['close'].rolling(length).mean()
    dev = 2.0 * df['close'].rolling(length).std()
    df['bb_upper'] = df['basis'] + dev
    df['bb_lower'] = df['basis'] - dev
    tr = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
    df['kc_upper'] = df['basis'] + (tr.rolling(length).mean() * 1.5)
    df['kc_lower'] = df['basis'] - (tr.rolling(length).mean() * 1.5)
    df['squeeze_on'] = (df['bb_upper'] < df['kc_upper']) & (df['bb_lower'] > df['kc_lower'])
    return df

def get_htf_trend(symbol):
    try:
        bars = exchange.fetch_ohlcv(symbol, '4h', limit=100)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ema_50'] = df['close'].ewm(span=50).mean()
        return "BULLISH" if df['close'].iloc[-1] > df['ema_50'].iloc[-1] else "BEARISH"
    except Exception: return "UNKNOWN"

# ── 🧠 REGIME OPTIMIZER (WITH ROI & PF) ────────────────────────────
def calculate_historical_edge(df, min_trades=50):
    df['atr'] = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1).ewm(span=14).mean()
    
    # Strictly Uninverted Directional Signals
    l_fvg, s_fvg = df['fvg_bull'] & (df['close'] > df['daily_vwap']), df['fvg_bear'] & (df['close'] < df['daily_vwap'])
    l_obv, s_obv = (df['obv'] > df['obv_ema']) & (df['close'] > df['daily_vwap']), (df['obv'] < df['obv_ema']) & (df['close'] < df['daily_vwap'])
    l_sqz, s_sqz = df['squeeze_on'] & (df['close'] > df['daily_vwap']), df['squeeze_on'] & (df['close'] < df['daily_vwap'])

    regimes = {
        'Regime 1 (FVG Sniper)': (l_fvg.shift(1), s_fvg.shift(1)),
        'Regime 2 (OBV Breakout)': (l_obv.shift(1), s_obv.shift(1)),
        'Regime 3 (Squeeze Trend)': (l_sqz.shift(1), s_sqz.shift(1))
    }
    
    test_multipliers = [1.50, 2.00, 2.50]
    best_mult, best_mode, best_exp, best_wr, best_pf, best_roi = None, None, 0.0, 0.0, 0.0, 0.0
    
    for mode_name, (l_sig, s_sig) in regimes.items():
        indices = df.index[l_sig | s_sig].tolist()
        for sl_m in test_multipliers:
            trades = []
            for idx in indices:
                if idx >= len(df) - 2: continue
                is_l = l_sig[idx]
                entry = df['close'].iloc[idx]
                atr = df['atr'].iloc[idx]
                if pd.isna(atr) or atr == 0: continue
                sl_dist = atr * sl_m
                cur_sl = entry - sl_dist if is_l else entry + sl_dist
                be_price = entry * 1.002 if is_l else entry * 0.998
                cat_tp = entry + (10.0 * atr) if is_l else entry - (10.0 * atr)
                best_px = entry
                be_triggered = False
                tr_r = 0.0
                
                for fwd in range(idx + 1, len(df)):
                    h, l = df['high'].iloc[fwd], df['low'].iloc[fwd]
                    if is_l:
                        if l <= cur_sl: tr_r = (cur_sl - entry) / sl_dist; break
                        if h >= cat_tp: tr_r = (cat_tp - entry) / sl_dist; break
                        best_px = max(best_px, h)
                        if (best_px - entry) >= (sl_dist * 1.5) and not be_triggered:
                            be_triggered = True
                            cur_sl = max(cur_sl, be_price)
                            tr_r += 0.75  # Simulate partial scale out at 1.5R
                    else:
                        if h >= cur_sl: tr_r = (entry - cur_sl) / sl_dist; break
                        if l <= cat_tp: tr_r = (entry - cat_tp) / sl_dist; break
                        best_px = min(best_px, l)
                        if (entry - best_px) >= (sl_dist * 1.5) and not be_triggered:
                            be_triggered = True
                            cur_sl = min(cur_sl, be_price)
                            tr_r += 0.75
                if tr_r != 0.0: trades.append(tr_r)
            
            if len(trades) >= min_trades:
                exp = sum(trades) / len(trades)
                wr = (sum(1 for t in trades if t > 0.05) / len(trades)) * 100
                gross_profit = sum(t for t in trades if t > 0)
                gross_loss = abs(sum(t for t in trades if t < 0))
                pf = gross_profit / gross_loss if gross_loss > 0 else 99.0
                total_roi = sum(trades)

                # STRICT FILTERS: 42% WR, +0.45R Exp, 1.5 PF, 20R Total
                if exp > 0.45 and wr > 42.0 and pf > 1.5 and total_roi > 20.0 and exp > best_exp:
                    best_exp, best_mult, best_mode, best_wr, best_pf, best_roi = exp, sl_m, mode_name, wr, pf, total_roi

    return best_mult, best_mode, best_exp, best_wr, best_roi

# ── Radar & Execution ──────────────────────────────────────────────
def scan_market_radar():
    print(f"📡 [RADAR] Sweeping Bybit for Top {RADAR_TOP_COINS} Liquid Movers...")
    try:
        now = time.time()
        expired = [sym for sym, expiry in edge_cooldowns.items() if now > expiry]
        for sym in expired: del edge_cooldowns[sym]

        tickers = exchange.fetch_tickers()
        valid_coins = []
        for symbol, data in tickers.items():
            if not symbol.endswith(':USDT'): continue
            if symbol in edge_cooldowns: continue 
            qv, lp, h24, l24 = float(data.get('quoteVolume', 0)), float(data.get('last', 0)), float(data.get('high', 0)), float(data.get('low', 0))
            if lp == 0 or l24 == 0 or qv < RADAR_MIN_VOLUME: continue
            valid_coins.append({'symbol': symbol, 'volatility': (h24 - l24) / l24, 'volume': qv})
            
        valid_coins.sort(key=lambda x: x['volume'], reverse=True)
        top_liquid_50 = valid_coins[:50] 
        top_liquid_50.sort(key=lambda x: x['volatility'], reverse=True)
        
        global active_watchlist
        active_watchlist = [c['symbol'] for c in top_liquid_50[:RADAR_TOP_COINS]]
        print(f"🎯 [RADAR LOCK] Liquid Targets: {[s.split('/')[0] for s in active_watchlist]}")
    except Exception: pass

def execute_trade(symbol, direction, size, entry, sl, tp):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        f_sz = float(exchange.amount_to_precision(symbol, size))
        f_sl, f_tp = str(float(exchange.price_to_precision(symbol, sl))), str(float(exchange.price_to_precision(symbol, tp)))
        f_px = float(exchange.price_to_precision(symbol, entry))
        exchange.set_margin_mode('isolated', symbol)
        exchange.set_leverage(10, symbol)
        order = exchange.create_order(symbol=symbol, type='limit', side=side, amount=f_sz, price=f_px, 
            params={'stopLoss': f_sl, 'takeProfit': f_tp, 'tpslMode': 'Full', 'slOrderType': 'Market', 'tpOrderType': 'Market', 'timeInForce': 'GTC'})
        return order, f_sz, float(f_sl), float(f_tp)
    except Exception: return None, None, None, None

# ── ⚡ 1-MINUTE FAST MANAGEMENT ────────────────────────────────────
def fast_management():
    if not pending_orders and not open_positions: return
    try:
        live_positions = exchange.fetch_positions()
        live_syms = {p['symbol'] for p in live_positions if float(p.get('contracts', 0)) > 0}

        # Sync New Orders & Send Alert
        for sym in list(pending_orders.keys()):
            if sym in live_syms:
                p = pending_orders.pop(sym)
                open_positions[sym] = p
                save_state()
                dir_icon = "🟢 LONG" if p['direction'] == 'LONG' else "🔴 SHORT"
                msg = (f"🚨 {dir_icon} EXECUTION: {sym.split('/')[0]}\n"
                       f"Entry: {p['entry']:.5f}\n"
                       f"Stop Loss: {p['current_sl']}\n"
                       f"Break Even At: {p['be_price']:.5f}\n"
                       f"Regime: {p['mode']}\n"
                       f"Backtest: WR {p['win_rate']:.1f}% | Exp +{p['expectancy']:.2f}R | ROI +{p['roi']:.2f}R")
                send_telegram(msg)
        
        # Sync Closed
        for sym in list(open_positions.keys()):
            if sym not in live_syms:
                del open_positions[sym]
                save_state()
                continue

        # Live Trailing & Scale Out Logic
        for symbol, pos in list(open_positions.items()):
            df = exchange.fetch_ohlcv(symbol, '1m', limit=5)
            if not df: continue
            current_price = df[-1][4]
            is_l = pos['direction'] == 'LONG'
            entry, sl_dist = pos['entry'], pos['sl_distance']
            diff = abs(current_price - entry)
            
            # SANITY CHECK 1: Time-Based Exit (24H / 86400s)
            if time.time() - pos.get('entry_time', time.time()) > 86400:
                exchange.create_market_order(symbol, 'sell' if is_l else 'buy', pos['size'], params={'reduceOnly': True})
                send_telegram(f"⏰ <b>TIME STOP HIT: {symbol}</b> (24H elapsed)")
                continue

            # SCALE OUT: 1.5R Hits -> Sell 50%, Move SL to BE
            if diff >= (sl_dist * 1.5) and not pos.get('scaled_out', False):
                half_size = float(exchange.amount_to_precision(symbol, pos['size'] / 2))
                exchange.create_market_order(symbol, 'sell' if is_l else 'buy', half_size, params={'reduceOnly': True})
                exchange.privatePostV5PositionTradingStop({'category': 'linear', 'symbol': exchange.market(symbol)['id'], 'side': 'Buy' if is_l else 'Sell', 'tpslMode': 'Full', 'stopLoss': str(pos['be_price'])})
                pos['scaled_out'] = True
                pos['current_sl'] = pos['be_price']
                save_state()
                send_telegram(f"💰 <b>SCALE OUT SECURED: {symbol} (+1.5R)</b>\nStop moved to Break Even.")
    except Exception: pass

# ── 📡 BACKGROUND SCANNER ──────────────────────────────────────────
def background_scanner():
    global is_scanning
    if is_scanning or is_kill_switch_active(): return
    is_scanning = True
    today_pnl = daily_pnl_tracker.get(date.today(), 0.0)

    try:
        scan_market_radar()
        if len(open_positions) + len(pending_orders) >= MAX_CONCURRENT: return

        for symbol in active_watchlist:
            if symbol in open_positions or symbol in pending_orders: continue
            htf_trend = get_htf_trend(symbol)

            if symbol in approved_coins:
                conf = approved_coins[symbol]
                opt_sl_m, mode, exp, wr, roi = conf['mult'], conf['mode'], conf['exp'], conf['wr'], conf['roi']
                print(f"  🔍 Hunting for {mode} entry on {symbol.split('/')[0]}...")
            else:
                df = fetch_deep_data(symbol, '15m', 3000)
                if df is None or len(df) < 1500: continue
                df = add_vwap(df); df = add_fvg_obv(df); df = add_squeeze(df)
                opt_sl_m, mode, exp, wr, roi = calculate_historical_edge(df, min_trades=50)
                
                if not opt_sl_m: 
                    print(f"  🚫 {symbol.split('/')[0]} FAILED: Not printing money. Burned.")
                    edge_cooldowns[symbol] = time.time() + 3600
                    continue
                print(f"  🌟 {symbol.split('/')[0]} APPROVED! Mode: {mode} | SL Mult: {opt_sl_m}x | Exp: +{exp:.2f}R")
                approved_coins[symbol] = {'mult': opt_sl_m, 'mode': mode, 'exp': exp, 'wr': wr, 'roi': roi}

            # Live Candle Check
            df = fetch_deep_data(symbol, '15m', 50)
            if df is None: continue
            df = add_vwap(df); df = add_fvg_obv(df); df = add_squeeze(df)
            
            c15m = df.iloc[-2]
            price = float(df.iloc[-1]['close'])
            atr = float(df['high'].iloc[-14:] - df['low'].iloc[-14:]).mean() # Simplified live ATR
            
            l_sig, s_sig = False, False
            if 'FVG' in mode: l_sig, s_sig = c15m['fvg_bull'] and price > c15m['daily_vwap'], c15m['fvg_bear'] and price < c15m['daily_vwap']
            elif 'OBV' in mode: l_sig, s_sig = c15m['obv'] > c15m['obv_ema'] and price > c15m['daily_vwap'], c15m['obv'] < c15m['obv_ema'] and price < c15m['daily_vwap']
            elif 'Squeeze' in mode: l_sig, s_sig = c15m['squeeze_on'] and price > c15m['daily_vwap'], c15m['squeeze_on'] and price < c15m['daily_vwap']

            # HTF God Mode Filter
            if htf_trend == "BEARISH": l_sig = False
            if htf_trend == "BULLISH": s_sig = False

            if not l_sig and not s_sig: continue
            
            risk = (P1_RISK if CURRENT_PHASE == 1 else P2_RISK) * (HOUSE_MONEY_MULTIPLIER if today_pnl >= HOUSE_MONEY_THRESHOLD else 1.0)
            direction = 'LONG' if l_sig else 'SHORT'
            sl_p = price - (opt_sl_m * atr) if l_sig else price + (opt_sl_m * atr)
            tp_p = price + (10.0 * atr) if l_sig else price - (10.0 * atr)
            sl_d = abs(price - sl_p)
            
            order, f_size, f_sl, f_tp = execute_trade(symbol, direction, risk / sl_d, price, sl_p, tp_p)
            if order:
                pending_orders[symbol] = {'direction': direction, 'entry': price, 'size': f_size, 'atr': atr, 'opt_sl_m': opt_sl_m,
                                          'current_sl': f_sl, 'sl_distance': sl_d, 'be_price': price * 1.002 if l_sig else price * 0.998, 
                                          'mode': mode, 'win_rate': wr, 'expectancy': exp, 'roi': roi, 'entry_time': time.time()}
                save_state()
    except Exception: pass
    finally: is_scanning = False

def trigger_scanner():
    threading.Thread(target=background_scanner).start()

# ── Main Loop ──────────────────────────────────────────────────────
if __name__ == '__main__':
    load_state()
    send_telegram("🤖 <b>Apex Beast V8.1 is ONLINE</b>\n📡 Scanning Top 15 Coins...\n📊 Volume Surge & Hunter Logs: Active")
    
    trigger_scanner() 
    schedule.every(1).minutes.do(fast_management)            
    schedule.every(15).minutes.at(":00").do(trigger_scanner) 
    schedule.every().day.at("00:05").do(lambda: (daily_pnl_tracker.clear(), approved_coins.clear(), edge_cooldowns.clear()))
    
    while True:
        schedule.run_pending()
        time.sleep(1)
