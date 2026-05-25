import ccxt.pro as ccxtpro  
import ccxt 
import pandas as pd
import numpy as np
import asyncio
import time
import os
import gc
import json
import pathlib
import requests
from datetime import datetime, timezone, date

# ── 🔥 APEX V1.0: SYNAPSE TRAIL PRO EDITION (5M MODE) 🔥 ──
market_data_cache = {}         
active_ws_tasks = {}       
active_watchlist = set()   
coin_tiers = {}            
edge_cooldowns = {}        
PNL_FILE = 'daily_pnl.json'

_tg_semaphore = None
_tg_message_count = 0
_tg_window_start = 0.0
_last_reset_date = None
processing_symbols = set()   # 🔒 CONCURRENCY LOCK

# ── Credentials & Config ───────────────────────────────────────────
BYBIT_API_KEY    = "FOqGNCN6gRxu4bqMqF"      
BYBIT_API_SECRET = "YmSWYNkQbVXYiFU5v0G3y3R405VLREGu7icy"   

TELEGRAM_BOT_TOKEN = "8955584540:AAHw7vbnRTWyO5pGOyvyWiMlPiagv7qpmOQ"  
TELEGRAM_CHAT_ID   = "1932328527"               

DAILY_KILL_SWITCH  = -125.0   # 5R Prop-Firm Hard Stop (Realized)
EQUITY_HARD_STOP   = -100.0   # 4R Prop-Firm Equity Circuit Breaker (Realized + Unrealized)
DEFENSIVE_DD_LIMIT = -75.0    # 3R Defensive Mode Trigger
DEFENSIVE_RISK     = 25.0     
BASE_RISK_PER_TRADE = 30.0    
MAX_CONCURRENT     = 3        
NEWS_BLACKOUT_MINUTES = 45    

SMC_MIN_VOLUME         = 250000000  
SMC_RADAR_TOP_COINS    = 5        
TREND_MIN_VOLUME       = 50000000   
TREND_RADAR_TOP_COINS  = 150        

VIP_SYMBOLS = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 'BNB/USDT:USDT', 'XRP/USDT:USDT'] 
CORR_GROUPS = [
    {'BTC/USDT:USDT', 'ETH/USDT:USDT'},   
    {'SOL/USDT:USDT', 'BNB/USDT:USDT'},   
]

open_positions       = {}
pending_orders       = {}  
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

# ── 🔥 SYSTEM HELPERS 🔥 ──
def load_daily_pnl():
    try:
        data = json.loads(pathlib.Path(PNL_FILE).read_text())
        if data.get('date') == str(date.today()):
            daily_pnl_tracker[date.today()] = data['pnl']
            daily_pnl_tracker['wins_today'] = data.get('wins', 0)
            daily_pnl_tracker['losses_today'] = data.get('losses', 0)
            daily_pnl_tracker['hourly_losses'] = data.get('hourly_losses', [])
            daily_pnl_tracker['consecutive_losses'] = data.get('consecutive_losses', 0)
            daily_pnl_tracker['equity_blown'] = data.get('equity_blown', False)
        
        for sym, ts in data.get('cooldowns', {}).items():
            if float(ts) > time.time():
                edge_cooldowns[sym] = float(ts)
    except Exception: pass

def save_daily_pnl():
    try:
        current_time = time.time()
        recent_hourly = [hl for hl in daily_pnl_tracker.get('hourly_losses', []) if current_time - hl[0] < 3600]
        daily_pnl_tracker['hourly_losses'] = recent_hourly

        pathlib.Path(PNL_FILE).write_text(json.dumps({
            'date': str(date.today()),
            'pnl': daily_pnl_tracker.get(date.today(), 0.0),
            'wins': daily_pnl_tracker.get('wins_today', 0),
            'losses': daily_pnl_tracker.get('losses_today', 0),
            'hourly_losses': recent_hourly,
            'consecutive_losses': daily_pnl_tracker.get('consecutive_losses', 0),
            'equity_blown': daily_pnl_tracker.get('equity_blown', False),
            'cooldowns': {k: v for k, v in edge_cooldowns.items() if v > current_time}
        }))
    except Exception: pass

def log_terminal(component, symbol, msg):
    now = datetime.now().strftime("%H:%M:%S")
    comp_padded = f"[{component}]".ljust(11)
    sym_padded = f"| {symbol.split(':')[0]} |".ljust(14) if symbol else "| SYSTEM |".ljust(14)
    print(f"[{now}] {comp_padded} {sym_padded} {msg}")

async def send_telegram(text):
    global _tg_semaphore, _tg_message_count, _tg_window_start
    if _tg_semaphore is None: _tg_semaphore = asyncio.Semaphore(3)
    
    now = time.time()
    if now - _tg_window_start > 60:
        _tg_message_count = 0
        _tg_window_start = now
    _tg_message_count += 1
    if _tg_message_count > 20: return
    
    async with _tg_semaphore:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try: await asyncio.to_thread(requests.post, url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'}, timeout=10)
        except Exception: pass
        await asyncio.sleep(0.1)

def is_kill_switch_active() -> bool:
    return daily_pnl_tracker.get(date.today(), 0.0) <= DAILY_KILL_SWITCH or daily_pnl_tracker.get('equity_blown', False)

def reset_daily_counters_if_needed():
    global _last_reset_date
    today = date.today()
    if _last_reset_date == today: return
    
    if daily_pnl_tracker.get('last_reset_date') != today:
        daily_pnl_tracker['wins_today'] = 0
        daily_pnl_tracker['losses_today'] = 0
        daily_pnl_tracker[today] = 0.0
        daily_pnl_tracker['hourly_losses'] = []
        daily_pnl_tracker['consecutive_losses'] = 0
        daily_pnl_tracker['equity_blown'] = False
        daily_pnl_tracker['last_reset_date'] = today
        edge_cooldowns.pop('__global_consec__', None)
        edge_cooldowns.pop('__global__', None)
    _last_reset_date = today

def is_news_blackout() -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() == 4 and now.day <= 7:
        nfp_time = now.replace(hour=13, minute=30, second=0, microsecond=0)
        if abs((now - nfp_time).total_seconds()) <= NEWS_BLACKOUT_MINUTES * 60:
            return True
    return False

def is_correlated_position_active(symbol, direction):
    for group in CORR_GROUPS:
        if symbol in group:
            for sym in group:
                if sym != symbol and sym in open_positions:
                    if open_positions[sym]['direction'] == direction:
                        return True
    return False

# ── 🔥 TA MATH & SYNAPSE TRAIL PRO [WAT] LOGIC 🔥 ──
def rma(series, length): return series.ewm(alpha=1/length, adjust=False).mean()

def calc_atr(df, length):
    prev_close = df['close'].shift(1)
    tr = pd.concat([df['high'] - df['low'], (df['high'] - prev_close).abs(), (df['low'] - prev_close).abs()], axis=1).max(axis=1)
    return rma(tr, length)

def calc_synapse_trail_pro(df):
    """
    Synapse Trail Pro [WillyAlgoTrader] Python Implementation
    Matches screenshot settings precisely.
    """
    atr_len = 13
    base_mult = 1.618
    trail_len = 21
    adx_len = 14
    chop_len = 14
    r2_len = 50
    min_quality = 75
    
    # --- ATR & Volatility Rank ---
    df['atr'] = calc_atr(df, atr_len)
    
    def calc_percentrank(x):
        if len(x) <= 1: return 50.0
        return (x < x.iloc[-1]).sum() / (len(x)-1) * 100.0
        
    df['vol_rank'] = df['atr'].rolling(100).apply(calc_percentrank, raw=False).fillna(50.0)
    
    def apply_adaptive_mult(rank):
        if rank < 30: return 0.8
        if rank > 70: return 1.25
        return 1.0
        
    df['eff_mult'] = base_mult * df['vol_rank'].apply(apply_adaptive_mult)
    
    # --- Trail & Bands ---
    df['trail_center'] = df['close'].ewm(span=trail_len, adjust=False).mean()
    df['raw_upper'] = df['trail_center'] + df['atr'] * df['eff_mult']
    df['raw_lower'] = df['trail_center'] - df['atr'] * df['eff_mult']
    
    n = len(df)
    upper = np.zeros(n)
    lower = np.zeros(n)
    direction = np.zeros(n)
    
    raw_u = df['raw_upper'].values
    raw_l = df['raw_lower'].values
    close = df['close'].values
    
    upper[0] = raw_u[0]
    lower[0] = raw_l[0]
    direction[0] = 1
    
    # Adaptive Ratchet Trail Logic
    for i in range(1, n):
        if np.isnan(raw_u[i]):
            upper[i], lower[i], direction[i] = upper[i-1], lower[i-1], direction[i-1]
            continue
            
        prev_u = upper[i-1] if not np.isnan(upper[i-1]) else raw_u[i]
        prev_l = lower[i-1] if not np.isnan(lower[i-1]) else raw_l[i]
        prev_dir = direction[i-1]
        
        # Direction Logic
        if close[i] > prev_u:
            curr_dir = 1
        elif close[i] < prev_l:
            curr_dir = -1
        else:
            curr_dir = prev_dir
            
        direction[i] = curr_dir
        dir_flipped = (curr_dir != prev_dir)
        
        # Ratchet Logic
        if curr_dir == 1:
            lower[i] = raw_l[i] if dir_flipped else max(raw_l[i], prev_l)
            upper[i] = raw_u[i]
        elif curr_dir == -1:
            upper[i] = raw_u[i] if dir_flipped else min(raw_u[i], prev_u)
            lower[i] = raw_l[i]
        else:
            upper[i] = raw_u[i]
            lower[i] = raw_l[i]

    df['upper'] = upper
    df['lower'] = lower
    df['dir'] = direction
    
    # --- Signal Triggers ---
    df['buy_signal'] = (df['dir'] == 1) & (df['dir'].shift(1) == -1)
    df['sell_signal'] = (df['dir'] == -1) & (df['dir'].shift(1) == 1)
    
    # --- Filters: Regime Analysis ---
    # 1. RSI (14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 2. ADX (14)
    up = df['high'].diff()
    down = -df['low'].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
    
    atr14 = pd.Series(tr).ewm(alpha=1/14, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    df['adx'] = dx.ewm(alpha=1/14, adjust=False).mean()
    df['adx_score'] = np.minimum(df['adx'] / 50.0 * 100.0, 100.0)
    
    # 3. Choppiness (14)
    tr_sum = pd.Series(tr).rolling(chop_len).sum()
    chop_range = df['high'].rolling(chop_len).max() - df['low'].rolling(chop_len).min()
    chop_raw = 100 * np.log10(tr_sum / chop_range) / np.log10(chop_len)
    df['chop_score'] = np.maximum(0.0, np.minimum(100.0, 100.0 - chop_raw))
    
    # 4. R2 (50)
    def rolling_r2(y):
        if len(y) < r2_len: return 0.0
        r = np.corrcoef(np.arange(r2_len), y)[0, 1]
        return (r**2) * 100.0 if not np.isnan(r) else 0.0
        
    df['r2_score'] = df['close'].rolling(r2_len).apply(rolling_r2, raw=True)
    
    # Composite Regime Score
    df['regime_score'] = (df['adx_score'] * 0.40) + (df['chop_score'] * 0.35) + (df['r2_score'] * 0.25)
    
    # --- HTF Bias & Break Strength ---
    # 100% Parity with Pine Script request.security("60", ta.ema(close, 50))
    
    # Temporarily set datetime index for resampling
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    
    # Resample to 1H, calculate 50 EMA, and merge back
    df_1h = df.set_index('datetime').resample('1h').agg({'close': 'last'}).dropna()
    df_1h['htf_ema'] = df_1h['close'].ewm(span=50, adjust=False).mean()
    
    # Map the 1H EMA back to the 5M timeframe and forward-fill
    df = df.join(df_1h[['htf_ema']], on='datetime')
    df['htf_ema'] = df['htf_ema'].ffill()
    
    df['htf_bull'] = df['close'] > df['htf_ema']
    df['htf_bear'] = df['close'] < df['htf_ema']
    
    # Cleanup temporary datetime column
    df.drop(columns=['datetime'], inplace=True)
    
    break_dist = np.where(df['dir'] == 1, df['close'] - df['upper'].shift(1), df['lower'].shift(1) - df['close'])
    df['break_strength'] = np.minimum(abs(break_dist) / df['atr'], 3.0) / 3.0 * 100.0
    
    # --- Quality Score Generation ---
    def get_q_score(row, is_buy):
        htf_part = 30.0 if (is_buy and row['htf_bull']) or (not is_buy and row['htf_bear']) else (0.0 if (is_buy and row['htf_bear']) or (not is_buy and row['htf_bull']) else 15.0)
        vol_part = 20.0 # Volume input turned off in screenshot -> defaults to 20
        rsi_part = 20.0 if (is_buy and row['rsi'] > 50) or (not is_buy and row['rsi'] < 50) else 0.0
        regime_part = row['regime_score'] * 0.20
        break_part = row['break_strength'] * 0.10
        return htf_part + vol_part + rsi_part + regime_part + break_part
        
    df['buy_quality'] = df.apply(lambda r: get_q_score(r, True) if r['buy_signal'] else np.nan, axis=1)
    df['sell_quality'] = df.apply(lambda r: get_q_score(r, False) if r['sell_signal'] else np.nan, axis=1)
    
    # Final Passes (Minimum 75 Quality + Hard Skip Choppy (<35 Regime))
    df['buy_pass'] = df['buy_signal'] & (df['buy_quality'] >= min_quality) & (df['regime_score'] >= 35)
    df['sell_pass'] = df['sell_signal'] & (df['sell_quality'] >= min_quality) & (df['regime_score'] >= 35)
    
    return df

def seed_historical_data(symbol, tf):
    try:
        # Pulled 1000 bars to ensure the 1H EMA has enough data to warm up.
        bars = rest_exchange.fetch_ohlcv(symbol, tf, limit=1000)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        for c in ['open', 'high', 'low', 'close']: df[c] = df[c].astype(float)
        market_data_cache[f"{symbol}_{tf}"] = df
    except Exception as e: pass

# ── 🔥 TRADE MANAGEMENT HELPERS 🔥 ──
async def modify_bybit_tpsl(symbol, direction, new_sl, current_tp):
    try:
        f_sl = float(rest_exchange.price_to_precision(symbol, new_sl))
        await asyncio.to_thread(
            rest_exchange.privatePostV5PositionTradingStop,
            {
                'category': 'linear', 'symbol': rest_exchange.market(symbol)['id'], 'positionIdx': 0, 'tpslMode': 'Full',
                'takeProfit': str(current_tp), 'stopLoss': str(f_sl), 'slOrderType': 'Market', 'tpOrderType': 'Market', 
                'slTriggerBy': 'LastPrice', 'tpTriggerBy': 'LastPrice'
            }
        )
        return f_sl
    except Exception: return None

def fetch_matched_pnl(sym, entry_time):
    try:
        entry_time_ms = int(entry_time * 1000)
        recs = rest_exchange.private_get_v5_position_closed_pnl({'category': 'linear', 'symbol': rest_exchange.market(sym)['id'], 'startTime': str(entry_time_ms), 'limit': 50}).get('result', {}).get('list', [])
        if recs:
            temp_pnl = 0.0
            for r in recs:
                close_time = int(r.get('createdTime', r.get('updatedTime', 0)))
                if close_time >= entry_time_ms: temp_pnl += float(r.get('closedPnl', 0.0))
            return temp_pnl
    except Exception: pass
    return None

async def handle_closed_trade(sym, pos):
    pnl = None
    start_t = pos['timestamp']  
    
    # Wait up to 3 minutes for the API
    for i in range(12):
        await asyncio.sleep(15) 
        pnl = await asyncio.to_thread(fetch_matched_pnl, sym, start_t)
        if pnl is not None: break
            
    base_sym = sym.split('/')[0]
    
    if pnl is None:
        is_l = pos['direction'] == 'LONG'
        peak = pos.get('peak_px', pos['entry'])
        max_r = (peak - pos['entry']) / pos['sl_distance'] if is_l else (pos['entry'] - peak) / pos['sl_distance']
        
        if max_r >= 1.95: 
            pnl = BASE_RISK_PER_TRADE * 2.0
        elif pos.get('be_activated'):
            pnl = BASE_RISK_PER_TRADE * 0.1
        else:
            pnl = -BASE_RISK_PER_TRADE
    
    net_pnl = pnl - pos.get('partial_pnl_fetched', 0.0)
    
    if abs(net_pnl) > 0.01:
        daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + net_pnl
        
    rr_captured = pnl / BASE_RISK_PER_TRADE
    
    if net_pnl < 0:
        daily_pnl_tracker.setdefault('hourly_losses', []).append((time.time(), net_pnl))
        daily_pnl_tracker['consecutive_losses'] = daily_pnl_tracker.get('consecutive_losses', 0) + 1
        if daily_pnl_tracker['consecutive_losses'] >= 3:
            edge_cooldowns['__global_consec__'] = time.time() + 5400  
            await send_telegram("🧊 <b>3 CONSECUTIVE LOSSES</b>. Bot cooling 90 min. Prop firm protection active.")
    else:
        daily_pnl_tracker['consecutive_losses'] = 0

    hourly_loss_sum = sum(abs(loss) for ts, loss in daily_pnl_tracker.get('hourly_losses', []) if time.time() - ts < 3600 and loss < 0)
    if hourly_loss_sum >= BASE_RISK_PER_TRADE * 2.5: 
        edge_cooldowns['__global__'] = time.time() + 3600
        await send_telegram("⏸️ <b>HOURLY LOSS LIMIT HIT</b>. Bot cooling 60 min.")

    if pnl > 0:
        daily_pnl_tracker['wins_today'] = daily_pnl_tracker.get('wins_today', 0) + 1
        msg = (f"🟢 TP HIT: {base_sym}\n"
               f"Net PnL: +{pnl:.2f} USD\n"
               f"Captured RR: +{rr_captured:.2f}R\n"
               f"Regime: {pos['regime_id']}")
        await send_telegram(msg)
        log_terminal("📊 SETTLED", sym, f"Position Closed. Total PnL: +{pnl:.2f} USD | RR: +{rr_captured:.2f}R")
        save_daily_pnl()
        edge_cooldowns[sym] = time.time() + 1800   
    else:
        daily_pnl_tracker['losses_today'] = daily_pnl_tracker.get('losses_today', 0) + 1
        msg = (f"🔴 SL HIT: {base_sym}\n"
               f"Net PnL: {pnl:.2f} USD\n"
               f"Captured RR: {rr_captured:.2f}R\n"
               f"Regime: {pos['regime_id']}")
        await send_telegram(msg)
        log_terminal("📊 SETTLED", sym, f"SL Hit. Total PnL: {pnl:.2f} USD | RR: {rr_captured:.2f}R")
        save_daily_pnl()
        edge_cooldowns[sym] = time.time() + 3600   

# ── 🔥 DYNAMIC RADAR 🔥 ──
async def dynamic_radar_loop():
    for sym in VIP_SYMBOLS:
        coin_tiers[sym] = {'tier': 1, 'max_spread': 0.05}

    while True:
        log_terminal("📡 RADAR", None, "Sweeping Bybit for 5M volume acceleration...")
        try:
            tickers = await asyncio.to_thread(rest_exchange.fetch_tickers)
            bucket_a_choppy, bucket_b_trending = [], []
            BLACKLIST = ['USDC', 'BUSD', 'EUR', 'GBP', 'WASH']
            
            for symbol, data in tickers.items():
                if not symbol.endswith(':USDT'): continue
                base_coin = symbol.split('/')[0]
                if base_coin in BLACKLIST: continue
                
                qv = float(data.get('quoteVolume', 0))
                lp = float(data.get('last', 0))
                if lp == 0 or qv < min(SMC_MIN_VOLUME, TREND_MIN_VOLUME): continue

                if qv >= SMC_MIN_VOLUME: coin_tiers[symbol] = {'tier': 1, 'max_spread': 0.05}
                elif qv >= 50000000: coin_tiers[symbol] = {'tier': 2, 'max_spread': 0.15}
                else: coin_tiers[symbol] = {'tier': 3, 'max_spread': 0.35}

                try:
                    bars = await asyncio.to_thread(rest_exchange.fetch_ohlcv, symbol, '5m', limit=288)
                    if not bars or len(bars) < 288: continue
                    
                    current_5m_vol = float(bars[-1][5])
                    avg_24h_vol = sum(float(b[5]) for b in bars) / 288
                    vol_accel = current_5m_vol / max(avg_24h_vol, 1e-8)
                    
                    coin_data = {'symbol': symbol, 'vol_accel': vol_accel}
                    if qv >= SMC_MIN_VOLUME: bucket_a_choppy.append(coin_data)
                    if qv >= TREND_MIN_VOLUME: bucket_b_trending.append(coin_data)
                except Exception: pass
                await asyncio.sleep(0.01) 

            bucket_a_choppy.sort(key=lambda x: x['vol_accel'], reverse=True)
            bucket_b_trending.sort(key=lambda x: x['vol_accel'], reverse=True)
            
            top_a = [c['symbol'] for c in bucket_a_choppy[:SMC_RADAR_TOP_COINS]]
            top_b = [c['symbol'] for c in bucket_b_trending[:TREND_RADAR_TOP_COINS]]
            
            new_watchlist = set(VIP_SYMBOLS + top_a + top_b)
            global active_watchlist
            
            dropped_count = 0
            for sym in list(active_watchlist):
                if sym not in new_watchlist and sym in active_ws_tasks:
                    if sym in open_positions or sym in pending_orders:
                        log_terminal("📡 RADAR", sym, "Dropping from watchlist skipped — active position.")
                        new_watchlist.add(sym)
                        continue
                    active_ws_tasks[sym].cancel()
                    active_ws_tasks.pop(sym, None)
                    market_data_cache.pop(f"{sym}_5m", None)
                    dropped_count += 1
                    
            added_count = 0
            for sym in new_watchlist:
                if sym not in active_ws_tasks:
                    await asyncio.to_thread(seed_historical_data, sym, '5m') 
                    task = asyncio.create_task(watch_ticker_stream(ws_exchange, sym))
                    active_ws_tasks[sym] = task
                    added_count += 1
                    await asyncio.sleep(0.2)
                    
            active_watchlist = new_watchlist
            log_terminal("📡 RADAR", None, f"Sweep Complete. Dropped {dropped_count}, Added {added_count}.")
            await asyncio.to_thread(gc.collect)
        except Exception: pass
        await asyncio.sleep(1800) 

# ── 🔥 EXECUTION ENGINE 🔥 ──
async def execute_trade_market(symbol, direction, risk_usd, trigger_px, sl_px, rr_target, regime, tier_grade, aligning_text, ta_logic):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        tier_info = coin_tiers.get(symbol, {'tier': 3, 'max_spread': 0.45})
        max_allowed_spread = tier_info['max_spread']
        
        ticker = await asyncio.to_thread(rest_exchange.fetch_ticker, symbol)
        ask, bid = float(ticker.get('ask', 0)), float(ticker.get('bid', 0))
        
        if ask > 0 and bid > 0:
            spread_pct = ((ask - bid) / bid) * 100
            if spread_pct > max_allowed_spread:
                log_terminal("🛡️ SHIELD", symbol, f"Spread {spread_pct:.3f}% > Tier {tier_info['tier']} Max. Trap aborted.")
                return

        sl_dist = abs(trigger_px - sl_px)
        if sl_dist == 0: return

        min_sl_pct = 0.0030  
        sl_pct = sl_dist / trigger_px

        if sl_pct < min_sl_pct:
            sl_dist = trigger_px * min_sl_pct
            sl_px = trigger_px - sl_dist if direction == 'LONG' else trigger_px + sl_dist

        tp_px = trigger_px + (sl_dist * rr_target) if direction == 'LONG' else trigger_px - (sl_dist * rr_target)

        fee_rate = 0.00055 
        true_risk_dist = sl_dist + (trigger_px * fee_rate * 2) 
        sz = risk_usd / true_risk_dist
        
        f_sz = float(rest_exchange.amount_to_precision(symbol, sz))
        actual_risk_check = float(f_sz) * sl_dist
        
        if actual_risk_check > risk_usd * 1.5:
            log_terminal("⚠️ SIZE CAP", symbol, f"Position size {f_sz} would risk ${actual_risk_check:.2f}. Capping.")
            f_sz = float(rest_exchange.amount_to_precision(symbol, (risk_usd * 1.2) / sl_dist))
            
        f_sl = str(float(rest_exchange.price_to_precision(symbol, sl_px)))
        f_tp = str(float(rest_exchange.price_to_precision(symbol, tp_px)))
        
        order = await asyncio.to_thread(
            rest_exchange.create_order,
            symbol=symbol, type='market', side=side, amount=f_sz, 
            params={
                'stopLoss': f_sl, 
                'takeProfit': f_tp, 
                'tpslMode': 'Full',
                'positionIdx': 0
            })
            
        registered = False
        for _ in range(5):
            await asyncio.sleep(1)
            try:
                pos_check = await asyncio.to_thread(rest_exchange.fetch_positions, [symbol])
                filled = [p for p in pos_check if float(p.get('contracts', 0)) > 0]
                if filled:
                    actual_entry = float(filled[0].get('entryPrice', trigger_px))
                    open_positions[symbol] = {
                        'id': order['id'], 
                        'timestamp': time.time(),
                        'direction': direction,
                        'entry': actual_entry,
                        'sl_distance': sl_dist,
                        'qty': f_sz,
                        'original_qty': f_sz,
                        'catastrophic_tp': f_tp,
                        'current_trail_sl': float(f_sl),    
                        'peak_px': actual_entry,              
                        'be_activated': False,
                        'regime_id': regime
                    }
                    log_terminal("✅ FILL", symbol, f"Market order confirmed in open_positions. Entry: {actual_entry}")
                    registered = True
                    break
            except Exception: pass

        if not registered:
            open_positions[symbol] = {
                'id': order['id'], 'timestamp': time.time(),
                'direction': direction, 'entry': trigger_px,
                'sl_distance': sl_dist, 'qty': f_sz, 'original_qty': f_sz,
                'catastrophic_tp': f_tp, 'current_trail_sl': float(f_sl),
                'peak_px': trigger_px, 'be_activated': False,
                'regime_id': regime
            }

        log_terminal("⚡ EXEC", symbol, f"Market Entry! Trigger: {trigger_px:.5f} | SL: {f_sl}")
        base_sym = symbol.split('/')[0]
        dir_icon = "🟢 LONG" if direction == 'LONG' else "🔴 SHORT"
        
        main_msg = (f"BYBIT 5M:\n"
                    f"🎯 [{tier_grade}] {dir_icon}: {base_sym}\n"
                    f"Type: ⚡ STP Engine\n"
                    f"Regime: {regime}\n"
                    f"Quantity: {f_sz}\n"
                    f"Trigger: {trigger_px:.5f}\n"
                    f"SL: {f_sl}\n"
                    f"TP (2R): {f_tp}\n"
                    f"Management: Strict 1:2 RR (1.5R BE + Fees)")
        await send_telegram(main_msg)

    except Exception as e: log_terminal("❌ ERROR", symbol, f"Market Execution failed: {e}")

# ── 🔥 DUAL-BRAIN ROUTING ENGINE 🔥 ──
async def analyze_structure(symbol):
    if symbol in processing_symbols: return
    processing_symbols.add(symbol)
    
    try:
        reset_daily_counters_if_needed()
        today_pnl = daily_pnl_tracker.get(date.today(), 0.0)
        
        if today_pnl <= -50.0:
            allocated_risk = 15.0
        elif today_pnl <= DEFENSIVE_DD_LIMIT:
            allocated_risk = BASE_RISK_PER_TRADE * 0.80
        else:
            allocated_risk = BASE_RISK_PER_TRADE

        if is_kill_switch_active() or (len(open_positions) + len(pending_orders)) >= MAX_CONCURRENT: return
        if symbol in pending_orders or symbol in open_positions: return
        if symbol in edge_cooldowns and time.time() < edge_cooldowns[symbol]: return

        if '__global__' in edge_cooldowns and time.time() < edge_cooldowns['__global__']: return
        if '__global_consec__' in edge_cooldowns and time.time() < edge_cooldowns['__global_consec__']: return
        
        if is_news_blackout(): return

        active_longs = sum(1 for p in list(open_positions.values()) + list(pending_orders.values()) if p['direction'] == 'LONG')
        active_shorts = sum(1 for p in list(open_positions.values()) + list(pending_orders.values()) if p['direction'] == 'SHORT')
        allow_long = active_longs < 2
        allow_short = active_shorts < 2

        if is_correlated_position_active(symbol, 'LONG') and allow_long: allow_long = False
        if is_correlated_position_active(symbol, 'SHORT') and allow_short: allow_short = False

        df_5m = market_data_cache.get(f"{symbol}_5m")
        # Ensure we have enough bars for the 1H EMA to stabilize
        if df_5m is None or len(df_5m) < 850: return
        df_5m = df_5m.copy()

        df_5m = calc_synapse_trail_pro(df_5m)
        c_bar = df_5m.iloc[-2] # Trigger off closed bar
        
        buy_signal = bool(c_bar['buy_pass'])
        sell_signal = bool(c_bar['sell_pass'])

        if buy_signal:
            direction = "LONG"
            signal_type = "STP Trend Continuation"      
        elif sell_signal:
            direction = "SHORT"
            signal_type = "STP Trend Continuation"
        else:
            return  

        if (direction == "LONG" and not allow_long) or (direction == "SHORT" and not allow_short): return

        pending_orders[symbol] = True

        tier_val = coin_tiers.get(symbol, {}).get('tier', 3)
        # Quality filter guarantees minimum 75 for it to even trigger.
        tier_grade = "A+" if tier_val == 1 and max(c_bar['buy_quality'], c_bar['sell_quality']) > 85 else "A"
        regime = f"Synapse Trail Pro [Regime Score: {c_bar['regime_score']:.1f}]"
        
        atr = float(c_bar['atr'])
        curr_px = float(df_5m['close'].iloc[-1])
        
        entry = curr_px
        sl_multiplier = 1.5 # From Input Settings (Custom SL xATR)
        
        if direction == "LONG": sl = entry - (sl_multiplier * atr)
        else: sl = entry + (sl_multiplier * atr)

        target_rr = 2.0 

        await execute_trade_market(
            symbol, direction, allocated_risk, entry, sl, target_rr, 
            regime, tier_grade, "Single TF (5M)", f"Dir: {direction} | QS: {max(c_bar['buy_quality'], c_bar['sell_quality']):.0f}/100"
        )
    finally:
        pending_orders.pop(symbol, None)
        processing_symbols.discard(symbol)

# ── 🔥 ASYNC WEBSOCKET ENGINE 🔥 ──
async def watch_ticker_stream(exchange, symbol):
    last_seen_ts = 0
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(symbol, '5m')
            new_bar = ohlcv[0]
            bar_ts = int(new_bar[0])
            
            df = market_data_cache.get(f"{symbol}_5m")
            if df is not None:
                if bar_ts > last_seen_ts:
                    if last_seen_ts > 0: 
                        new_row = pd.DataFrame(
                            [[new_bar[0], new_bar[1], new_bar[2], new_bar[3], new_bar[4], new_bar[5]]],
                            columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                        )
                        df = pd.concat([df, new_row], ignore_index=True)
                        # Keep 1000 bars to supply the 1H EMA requirement
                        df = df.tail(1000).reset_index(drop=True)
                        market_data_cache[f"{symbol}_5m"] = df
                        await analyze_structure(symbol)
                    else:
                        df.iloc[-1] = [new_bar[0], new_bar[1], new_bar[2], new_bar[3], new_bar[4], new_bar[5]]
                        market_data_cache[f"{symbol}_5m"] = df
                    last_seen_ts = bar_ts
                else:
                    df.iloc[-1] = [new_bar[0], new_bar[1], new_bar[2], new_bar[3], new_bar[4], new_bar[5]]
                    market_data_cache[f"{symbol}_5m"] = df
                    
        except asyncio.CancelledError: break 
        except Exception: await asyncio.sleep(2)

# ── 🔥 MANAGEMENT LOOP 🔥 ──
async def fast_management_loop():
    while True:
        try:
            pos_data = []
            try:
                pos_data = await asyncio.to_thread(rest_exchange.fetch_positions)
            except Exception: pass
            
            live_syms = {p['symbol']: p for p in pos_data if float(p.get('contracts', 0)) > 0}
            now = time.time()
            
            unrealized_pnl = sum(
                float(p.get('unrealisedPnl', p.get('unrealizedPnl', p.get('info', {}).get('unrealisedPnl', 0.0))))
                for p in pos_data if float(p.get('contracts', 0)) > 0
            )
            realized_pnl = daily_pnl_tracker.get(date.today(), 0.0)
            live_equity_pnl = realized_pnl + unrealized_pnl
            
            if live_equity_pnl <= EQUITY_HARD_STOP and not daily_pnl_tracker.get('equity_blown', False):
                daily_pnl_tracker['equity_blown'] = True
                log_terminal("🛑 EQUITY STOP", None, f"Live equity {live_equity_pnl:.2f} breached {EQUITY_HARD_STOP}. Trading halted.")
                await send_telegram(f"🚨 <b>EQUITY CIRCUIT BREAKER TRIPPED</b> 🚨\nRealized: {realized_pnl:.2f}\nUnrealized: {unrealized_pnl:.2f}\nTotal Equity: {live_equity_pnl:.2f}\nAll pending orders cancelled. New entries halted.")

            open_keys = list(open_positions.keys())
            if open_keys:
                tickers = await asyncio.to_thread(rest_exchange.fetch_tickers, open_keys)
                for sym in open_keys:
                    if sym not in live_syms:
                        if now - open_positions[sym]['timestamp'] < 30:
                            continue
                        
                        pos = open_positions.pop(sym)
                        asyncio.create_task(handle_closed_trade(sym, pos))
                        continue
                    
                    pos = open_positions[sym]
                    if sym not in tickers: continue
                    
                    cur_px = float(tickers[sym]['last'])
                    is_l = pos['direction'] == 'LONG'
                    pos['peak_px'] = max(pos['peak_px'], cur_px) if is_l else min(pos['peak_px'], cur_px)
                    current_r = ((cur_px - pos['entry']) if is_l else (pos['entry'] - cur_px)) / pos['sl_distance']

                    if 'Synapse Trail Pro' in pos['regime_id']:
                        if current_r >= 1.5 and not pos.get('be_activated', False):
                            fee_buffer = pos['entry'] * 0.0011 
                            be_px = pos['entry'] + fee_buffer if is_l else pos['entry'] - fee_buffer
                            
                            try:
                                await modify_bybit_tpsl(sym, pos['direction'], be_px, pos['catastrophic_tp'])
                                pos['current_trail_sl'] = be_px
                                pos['be_activated'] = True
                                await send_telegram(f"🛡️ BE LOCKED: {sym.split('/')[0]}\nPrice reached TP1 (1.5R). Stop moved to Entry + Fees.")
                            except Exception: pass

        except Exception as e: 
            log_terminal("❌ ERROR", None, f"Mgmt Loop Crash: {e}")
        await asyncio.sleep(5) 

async def sync_open_positions_on_startup():
    try:
        if not rest_exchange.markets:
            await asyncio.to_thread(rest_exchange.load_markets)
            
        pos_data = await asyncio.to_thread(rest_exchange.fetch_positions)
        for p in pos_data:
            if float(p.get('contracts', 0)) > 0:
                sym = p.get('symbol')
                if sym not in rest_exchange.markets:
                    raw_id = p.get('info', {}).get('symbol', '')
                    sym = next(
                        (s for s, m in rest_exchange.markets.items() 
                         if m.get('id') == raw_id),
                        None
                    )
                if not sym: continue
                    
                open_positions[sym] = {
                    'id': 'recovered',
                    'timestamp': time.time(),
                    'direction': 'LONG' if p.get('side') == 'long' else 'SHORT',
                    'entry': float(p.get('entryPrice', 0)),
                    'sl_distance': 1.0,  
                    'qty': float(p.get('contracts', 0)),
                    'original_qty': float(p.get('contracts', 0)),
                    'catastrophic_tp': '0',
                    'current_trail_sl': 0.0,
                    'peak_px': float(p.get('entryPrice', 0)),
                    'be_activated': False,
                    'regime_id': 'RECOVERED'
                }
    except Exception: pass

async def main():
    os.system('cls' if os.name == 'nt' else 'clear') 
    print("======================================================")
    print("  🤖 APEX V1.0 : SYNAPSE TRAIL PRO (5M MODE)          ")
    print("======================================================\n")
    load_daily_pnl()
    await sync_open_positions_on_startup()
    await send_telegram(f"🤖 <b>Apex V1.0 ONLINE</b>\nSynapse Trail Pro [WAT] Active.")
    await asyncio.gather(dynamic_radar_loop(), fast_management_loop())

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
