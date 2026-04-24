import ccxt
import pandas as pd
import numpy as np
import requests
import schedule
import time
from datetime import datetime, timezone, date, timedelta

# ══════════════════════════════════════════════════════════════════════════════
#  APEX SNIPER v7.2.9 PRO  ——  CFT-PASSING EDITION
# ══════════════════════════════════════════════════════════════════════════════

# ── Credentials ───────────────────────────────────────────────────────────────
BYBIT_API_KEY      = "FOqGNCN6gRxu4bqMqF"
BYBIT_API_SECRET   = "YmSWYNkQbVXYiFU5v0G3y3R405VLREGu7icy"
TELEGRAM_BOT_TOKEN = "8734785957:AAGzU-KPRY4mzXARxyTpLSHGemFtJ7AEsUQ"
TELEGRAM_CHAT_ID   = "1932328527"
REPLIT_WEBHOOK_URL = "https://dcf37de3-95b1-4275-aad3-54160dffeae5-00-1dydpq6kaysyl.riker.replit.dev/api/webhook/trade"

# ── Indicator Params ──────────────────────────────────────────────────────────
ST_FACTOR   = 2.0
ST_PERIOD   = 14
WMA_LENGTH  = 14
EMA_LENGTH  = 3
ATR_PERIOD  = 14
FRACTAL_R   = 3
TP1_MULT    = 1.0
TRAIL_MULT  = 0.10
CAT_MULT    = 10.0

# ── Symbol Universe ───────────────────────────────────────────────────────────
PER_SYMBOL_CONFIG = {
    'ETH/USDT:USDT': (1.00, 3.00, TRAIL_MULT, 20.0, 15.0),
    'SOL/USDT:USDT': (2.00, 3.00, TRAIL_MULT, 20.0, 15.0),
    'LTC/USDT:USDT': (2.00, 3.00, TRAIL_MULT, 20.0, 15.0),
    'ZEC/USDT:USDT': (2.00, 3.00, TRAIL_MULT, 20.0, 15.0),
}
SYMBOLS = list(PER_SYMBOL_CONFIG.keys())

# ── CFT Challenge Parameters ──────────────────────────────────────────────────
CURRENT_PHASE    = 1
CFT_ACCOUNT_SIZE = 5000.0
CFT_MDD_LIMIT    = 600.0
CFT_DAILY_LIMIT  = 250.0

# ── Risk Management ───────────────────────────────────────────────────────────
BASE_RISK_USD         = 20.0
MAX_CONCURRENT        = 2
MAX_NOTIONAL_USD      = 10000.0
FEE_CAP_FRAC          = 0.55
HOUSE_MONEY_THRESHOLD = 75.0
HOUSE_MONEY_MULT      = 1.5
DAILY_KILL_SWITCH     = -53.0
EQUITY_FLOOR_HALT     = -300.0

# ── Order Config ──────────────────────────────────────────────────────────────
BYBIT_MAKER_FEE    = 0.00020
BYBIT_TAKER_FEE    = 0.00055
LIMIT_FILL_TIMEOUT = 900

# ── Runtime State ─────────────────────────────────────────────────────────────
open_positions    = {}
pending_orders    = {}
daily_pnl_tracker = {}
equity_estimate   = CFT_ACCOUNT_SIZE
peak_equity       = CFT_ACCOUNT_SIZE
floor_halt_days   = set()

# ── Exchange Setup ────────────────────────────────────────────────────────────
exchange = ccxt.bybit({
    'apiKey'        : BYBIT_API_KEY,
    'secret'        : BYBIT_API_SECRET,
    'enableRateLimit': True,
    'options'       : {'defaultType': 'swap'},
})
exchange.enable_demo_trading(True)   # ← SET TO False FOR LIVE TRADING
exchange.load_markets()

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING & UTILS
# ══════════════════════════════════════════════════════════════════════════════

def log(msg: str, section: str = "BOT"):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}][{section}] {msg}", flush=True)

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'}, timeout=10)
    except:
        pass

def today() -> date: return date.today()
def utcnow() -> datetime: return datetime.now(timezone.utc)
def get_today_pnl() -> float: return daily_pnl_tracker.get(today(), 0.0)

def record_pnl(pnl_usd: float):
    global equity_estimate, peak_equity
    t = today()
    daily_pnl_tracker[t] = daily_pnl_tracker.get(t, 0.0) + pnl_usd
    equity_estimate += pnl_usd
    if equity_estimate > peak_equity: peak_equity = equity_estimate

def current_drawdown() -> float: return equity_estimate - peak_equity
def is_halted() -> bool: return get_today_pnl() <= DAILY_KILL_SWITCH or today() in floor_halt_days
def house_money_arm() -> float: return HOUSE_MONEY_MULT if get_today_pnl() >= HOUSE_MONEY_THRESHOLD else 1.0

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATOR MATH
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(symbol: str, timeframe: str = '15m', limit: int = 300):
    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        for c in ['open', 'high', 'low', 'close']: df[c] = df[c].astype(float)
        return df
    except: return None

def calc_atr(df: pd.DataFrame, length: int) -> pd.Series:
    pc = df['close'].shift(1)
    tr = pd.concat([df['high'] - df['low'], (df['high'] - pc).abs(), (df['low'] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def algoalpha_baseline(df: pd.DataFrame) -> pd.Series:
    st_atr = calc_atr(df, ST_PERIOD)
    hl2 = (df['high'] + df['low']) / 2.0
    upper, lower = (hl2 + ST_FACTOR * st_atr).values, (hl2 - ST_FACTOR * st_atr).values
    close, n = df['close'].values, len(df)
    lo, up = np.zeros(n), np.zeros(n)
    lo[0], up[0] = lower[0], upper[0]
    for i in range(1, n):
        lo[i] = lower[i] if (lower[i] > lo[i-1] or close[i-1] < lo[i-1]) else lo[i-1]
        up[i] = upper[i] if (upper[i] < up[i-1] or close[i-1] > up[i-1]) else up[i-1]
    mid = (pd.Series(lo, index=df.index) + pd.Series(up, index=df.index)) / 2.0
    w = np.arange(1, WMA_LENGTH + 1)
    wma = mid.rolling(WMA_LENGTH).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)
    return wma.ewm(span=EMA_LENGTH, adjust=False).mean()

def calc_smc_structure(df: pd.DataFrame) -> pd.DataFrame:
    window = FRACTAL_R * 2 + 1
    sh = pd.Series(np.where(df['high'].shift(FRACTAL_R) == df['high'].rolling(window=window).max(), df['high'].shift(FRACTAL_R), np.nan), index=df.index).ffill()
    sl = pd.Series(np.where(df['low'].shift(FRACTAL_R) == df['low'].rolling(window=window).min(), df['low'].shift(FRACTAL_R), np.nan), index=df.index).ffill()
    trend = pd.Series(np.nan, index=df.index)
    trend.loc[df['close'] > sh] = 1.0
    trend.loc[df['close'] < sl] = -1.0
    df['smc_trend'] = trend.ffill().fillna(0).astype(int)
    return df

def compute_signals(symbol: str):
    df = fetch_ohlcv(symbol, '15m', 300)
    if df is None or len(df) < 150: return None
    df['atr'] = calc_atr(df, ATR_PERIOD)
    df['tL'] = algoalpha_baseline(df)
    df = calc_smc_structure(df)
    atr, price, smc = float(df.iloc[-2]['atr']), float(df.iloc[-2]['close']), int(df.iloc[-2]['smc_trend'])
    tL = df['tL']
    if np.isnan(atr) or atr < price * 0.00005: return None
    
    # Logic for signals
    algo_long = (tL.iloc[-2] > tL.iloc[-3]) and (tL.iloc[-3] <= tL.iloc[-4])
    algo_short = (tL.iloc[-2] < tL.iloc[-3]) and (tL.iloc[-3] >= tL.iloc[-4])
    if algo_short and (smc == -1): side = 'LONG'
    elif algo_long and (smc == 1): side = 'SHORT'
    else: return None

    sl_m, tp_m, tr_m, p1_r, p2_r = PER_SYMBOL_CONFIG[symbol]
    risk_usd = (p1_r if CURRENT_PHASE == 1 else p2_r) * house_money_arm()
    return side, price, atr, sl_m, risk_usd

# ══════════════════════════════════════════════════════════════════════════════
#  EXECUTION ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def place_limit_entry(symbol: str, direction: str, lot: float, entry_px: float, sl_px: float, cat_tp_px: float):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        fmt_lot = float(exchange.amount_to_precision(symbol, lot))
        fmt_entry = float(exchange.price_to_precision(symbol, entry_px))
        fmt_sl = str(float(exchange.price_to_precision(symbol, sl_px)))
        fmt_tp = str(float(exchange.price_to_precision(symbol, cat_tp_px)))
        
        order = exchange.create_order(
            symbol=symbol, type='limit', side=side, amount=fmt_lot, price=fmt_entry, 
            params={'timeInForce': 'PostOnly', 'stopLoss': fmt_sl, 'takeProfit': fmt_tp, 'tpslMode': 'Full'}
        )
        return order.get('id'), fmt_lot, fmt_entry
    except Exception as e:
        log(f"Order FAILED [{symbol}]: {e}", "ORDER")
        return None, None, None

def update_trailing_sl(symbol: str, direction: str, new_sl: float, cat_tp: float) -> float | None:
    try:
        market_id, bybit_side = exchange.market(symbol)['id'], ('Buy' if direction == 'LONG' else 'Sell')
        fmt_sl = float(exchange.price_to_precision(symbol, new_sl))
        exchange.privatePostV5PositionTradingStop({
            'category': 'linear', 'symbol': market_id, 'side': bybit_side, 'tpslMode': 'Full',
            'takeProfit': str(float(exchange.price_to_precision(symbol, cat_tp))),
            'stopLoss': str(fmt_sl), 'slOrderType': 'Market',
        })
        return fmt_sl
    except: return None

def check_pending_orders():
    now = utcnow().timestamp()
    for sym in list(pending_orders.keys()):
        po = pending_orders[sym]
        if po['expires_at'] <= now:
            try: exchange.cancel_order(po['order_id'], sym)
            except: pass
            del pending_orders[sym]
            continue
        try:
            order = exchange.fetch_order(po['order_id'], sym)
            if order.get('status') == 'closed':
                fill_px = float(order.get('average') or order.get('price'))
                p = po['params']
                is_long = p['direction'] == 'LONG'
                sl_px = fill_px - p['atr'] * p['sl_m'] if is_long else fill_px + p['atr'] * p['sl_m']
                cat_tp = fill_px + p['atr'] * CAT_MULT if is_long else fill_px - p['atr'] * CAT_MULT
                
                open_positions[sym] = {
                    'direction': p['direction'], 'entry': fill_px, 'atr': p['atr'],
                    'sl_m': p['sl_m'], 'current_sl': sl_px, 'catastrophic_tp': cat_tp,
                    'best_price': fill_px, 'trail_active': False, 'lot': float(order.get('filled')),
                    'risk_usd': p['risk_usd']
                }
                del pending_orders[sym]
                
                coin = sym.split('/')[0]
                log(f"{coin} Entry Filled @ {fill_px}", "EXEC")
                
                dir_icon = '🟢 LONG' if is_long else '🔴 SHORT'
                send_telegram(f"<b>🎯 SETUP DETECTED & FILLED — {coin}</b>\nSetup: {dir_icon}\nEntry: <code>{fill_px}</code>\nSL: <code>{sl_px:.4f}</code>")
        except: pass

def sync_open_positions():
    try:
        live = exchange.fetch_positions()
        live_syms = {p['symbol'] for p in live if float(p.get('contracts', 0)) > 0}
    except: return
    for sym in list(open_positions.keys()):
        if sym not in live_syms:
            pos = open_positions.pop(sym)
            try:
                res = exchange.private_get_v5_position_closed_pnl({'category': 'linear', 'symbol': exchange.market(sym)['id'], 'limit': 1})
                pnl = float(res.get('result', {}).get('list', [{}])[0].get('closedPnl', 0.0))
            except: pnl = 0.0
            
            record_pnl(pnl)
            coin = sym.split('/')[0]
            log(f"{coin} Closed | PnL: ${pnl:.2f}", "EXEC")
            
            send_telegram(f"<b>✅ TRADE CLOSED — {coin}</b>\nSettled Net PnL: <b>${pnl:.2f}</b>")

def manage_trailing_stops():
    for sym, pos in list(open_positions.items()):
        try:
            df = fetch_ohlcv(sym, '1m', 5)
            if df is None: continue
            is_long = pos['direction'] == 'LONG'
            live_close, live_hi, live_lo = float(df.iloc[-1]['close']), float(df.iloc[-1]['high']), float(df.iloc[-1]['low'])
            
            new_best = max(pos['best_price'], live_hi) if is_long else min(pos['best_price'], live_lo)
            pos['best_price'] = new_best
            
            just_activated = False
            if not pos['trail_active'] and abs(new_best - pos['entry']) >= TP1_MULT * pos['atr']:
                pos['trail_active'] = True
                just_activated = True
            
            if pos['trail_active']:
                raw_sl = (new_best - TRAIL_MULT * pos['atr']) if is_long else (new_best + TRAIL_MULT * pos['atr'])
                if (is_long and raw_sl > pos['current_sl']) or (not is_long and raw_sl < pos['current_sl']):
                    fmt_sl = update_trailing_sl(sym, pos['direction'], raw_sl, pos['catastrophic_tp'])
                    
                    if fmt_sl: 
                        pos['current_sl'] = fmt_sl
                        
                        if just_activated:
                            coin = sym.split('/')[0]
                            dir_icon = '▲ LONG' if is_long else '▼ SHORT'
                            
                            if is_long:
                                locked_pts = fmt_sl - pos['entry']
                                pos_str = "above"
                            else:
                                locked_pts = pos['entry'] - fmt_sl
                                pos_str = "below"
                                
                            msg = (f"<b>🛡️ FREE RIDE SECURE</b>\n"
                                   f"🔄 Trail Updated - {coin}\n"
                                   f"{dir_icon}\n"
                                   f"New SL: <code>{fmt_sl}</code>\n"
                                   f"Locked: {locked_pts:.4f} pts {pos_str} entry")
                            send_telegram(msg)
                            just_activated = False
        except: pass

def check_signals():
    log("Scanning coins...", "SCAN")
    if is_halted() or (len(open_positions) + len(pending_orders) >= MAX_CONCURRENT): return
    
    for sym in SYMBOLS:
        if sym in open_positions or sym in pending_orders: 
            continue
        
        result = compute_signals(sym)
        if not result: continue
        
        direction, entry_px, atr, sl_m, risk_usd = result
        sl_px = entry_px - sl_m * atr if direction == 'LONG' else entry_px + sl_m * atr
        cat_tp_px = entry_px + CAT_MULT * atr if direction == 'LONG' else entry_px - CAT_MULT * atr
        
        lot = risk_usd / abs(entry_px - sl_px)
        order_id, fmt_lot, fmt_entry = place_limit_entry(sym, direction, lot, entry_px, sl_px, cat_tp_px)
        
        if order_id:
            pending_orders[sym] = {
                'order_id': order_id, 'expires_at': utcnow().timestamp() + LIMIT_FILL_TIMEOUT,
                'entry_px': fmt_entry, 'lot': fmt_lot,
                'params': {'direction': direction, 'atr': atr, 'sl_m': sl_m, 'risk_usd': risk_usd}
            }

def fast_management():
    sync_open_positions()
    check_pending_orders()
    manage_trailing_stops()

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    log("APEX v7.2.9 PRO Booting...", "SYS")
    
    # Send Startup Message to Telegram
    send_telegram("🚀 <b>APEX SNIPER v7.2.9 PRO — ONLINE</b>\nSystem is monitoring markets and awaiting entry signals.")

    schedule.every(15).seconds.do(fast_management)
    for t in [":00", ":15", ":30", ":45"]:
        schedule.every().hour.at(t).do(check_signals)

    while True:
        # Keep logs moving every minute so Railway knows you're alive
        if datetime.now().second == 0:
            print(f"[HEARTBEAT] {datetime.now().strftime('%H:%M')} - Bot is active...", flush=True)
            
        try:
            schedule.run_pending()
        except Exception as e:
            log(f"Error: {e}", "LOOP")
            
        time.sleep(1)
