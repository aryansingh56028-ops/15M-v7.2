import ccxt
import pandas as pd
import numpy as np
import requests
import schedule
import time
from datetime import datetime, timezone, date, timedelta

# ══════════════════════════════════════════════════════════════════════════════
#  APEX SNIPER v7.2.8 PRO  ——  CFT-PASSING EDITION
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
CFT_P1_TARGET    = 5400.0
CFT_P2_TARGET    = 5600.0
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
p1_notified       = False
p2_notified       = False

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

def send_webhook(data: dict):
    if not REPLIT_WEBHOOK_URL: return
    try: requests.post(REPLIT_WEBHOOK_URL, json=data, timeout=5)
    except: pass

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
def is_kill_switch_active() -> bool: return get_today_pnl() <= DAILY_KILL_SWITCH
def is_floor_halted() -> bool: return today() in floor_halt_days
def is_halted() -> bool: return is_kill_switch_active() or is_floor_halted()
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
    except:
        return None

def calc_atr(df: pd.DataFrame, length: int) -> pd.Series:
    pc = df['close'].shift(1)
    tr = pd.concat([df['high'] - df['low'], (df['high'] - pc).abs(), (df['low'] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def calc_wma(series: pd.Series, length: int) -> pd.Series:
    w = np.arange(1, length + 1)
    return series.rolling(length).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)

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
    return calc_wma(mid, WMA_LENGTH).ewm(span=EMA_LENGTH, adjust=False).mean()

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
    
    bar, atr, price, smc = df.iloc[-2], float(df.iloc[-2]['atr']), float(df.iloc[-2]['close']), int(df.iloc[-2]['smc_trend'])
    tL = df['tL']
    
    if np.isnan(atr) or atr < price * 0.00005: return None
    
    algo_long = (tL.iloc[-2] > tL.iloc[-3]) and (tL.iloc[-3] <= tL.iloc[-4])
    algo_short = (tL.iloc[-2] < tL.iloc[-3]) and (tL.iloc[-3] >= tL.iloc[-4])
    long_sig = algo_short and (smc == -1)
    short_sig = algo_long and (smc == 1)
    
    if not long_sig and not short_sig: return None
    
    sl_m, tp_m, tr_m, p1_r, p2_r = PER_SYMBOL_CONFIG[symbol]
    base_risk = p1_r if CURRENT_PHASE == 1 else p2_r
    risk_usd = base_risk * house_money_arm()
    return 'LONG' if long_sig else 'SHORT', price, atr, sl_m, tr_m, risk_usd

# ══════════════════════════════════════════════════════════════════════════════
#  ORDER EXECUTION & MGMT
# ══════════════════════════════════════════════════════════════════════════════

def calc_lot(entry: float, sl: float, risk_usd: float) -> float:
    sl_dist = abs(entry - sl)
    if sl_dist == 0: return 0.0
    lot = risk_usd / sl_dist
    return min(lot, MAX_NOTIONAL_USD / entry)

def fee_check(lot: float, price: float, risk_usd: float) -> bool:
    return (lot * price * (BYBIT_MAKER_FEE + BYBIT_TAKER_FEE)) <= risk_usd * FEE_CAP_FRAC

def place_limit_entry(symbol: str, direction: str, lot: float, entry_px: float, sl_px: float, cat_tp_px: float):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        fmt_lot = float(exchange.amount_to_precision(symbol, lot))
        fmt_entry = float(exchange.price_to_precision(symbol, entry_px))
        order = exchange.create_order(symbol=symbol, type='limit', side=side, amount=fmt_lot, price=fmt_entry, params={'timeInForce': 'PostOnly'})
        return order.get('id'), fmt_lot, fmt_entry
    except Exception as e:
        log(f"Order FAILED [{symbol}]: {e}", "ORDER")
        return None, None, None

def set_tpsl(symbol: str, direction: str, sl_px: float, cat_tp_px: float) -> bool:
    try:
        market_id, bybit_side = exchange.market(symbol)['id'], ('Buy' if direction == 'LONG' else 'Sell')
        exchange.privatePostV5PositionTradingStop({
            'category': 'linear', 'symbol': market_id, 'side': bybit_side, 'tpslMode': 'Full',
            'takeProfit': str(float(exchange.price_to_precision(symbol, cat_tp_px))),
            'stopLoss': str(float(exchange.price_to_precision(symbol, sl_px))), 'slOrderType': 'Market',
        })
        return True
    except: return False

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

def close_market(symbol: str, direction: str) -> float | None:
    side = 'sell' if direction == 'LONG' else 'buy'
    try:
        pos_list = exchange.fetch_positions([symbol])
        if not pos_list or float(pos_list[0].get('contracts', 0)) <= 0: return None
        exchange.create_order(symbol=symbol, type='market', side=side, amount=float(pos_list[0].get('contracts')), params={'reduceOnly': True})
        time.sleep(2)
        res = exchange.private_get_v5_position_closed_pnl({'category': 'linear', 'symbol': exchange.market(symbol)['id'], 'limit': 1})
        records = res.get('result', {}).get('list', [])
        return float(records[0].get('closedPnl', 0.0)) if records else 0.0
    except: return None

def check_equity_floor():
    dd = current_drawdown()
    if dd <= EQUITY_FLOOR_HALT and today() not in floor_halt_days:
        floor_halt_days.add(today())
        send_telegram(f"🛑 <b>EQUITY FLOOR TRIPPED</b>\nDD from peak: <code>${dd:.2f}</code>. Halting bot.")
        for sym, po in list(pending_orders.items()):
            try: exchange.cancel_order(po['order_id'], sym)
            except: pass
        pending_orders.clear()
        
        for sym, pos in list(open_positions.items()):
            pnl = close_market(sym, pos['direction'])
            if pnl is not None: record_pnl(pnl)
        open_positions.clear()

def check_pending_orders():
    now = utcnow().timestamp()
    for sym in list(pending_orders.keys()):
        po = pending_orders[sym]
        coin = sym.split('/')[0]
        if po['expires_at'] <= now:
            try: exchange.cancel_order(po['order_id'], sym)
            except: pass
            del pending_orders[sym]
            continue
            
        try:
            order = exchange.fetch_order(po['order_id'], sym)
            if order.get('status') == 'closed':
                fill_px = float(order.get('average') or order.get('price') or po['entry_px'])
                p = po['params']
                is_long = p['direction'] == 'LONG'
                sl_px = fill_px - p['atr'] * p['sl_m'] if is_long else fill_px + p['atr'] * p['sl_m']
                cat_tp = fill_px + p['atr'] * CAT_MULT if is_long else fill_px - p['atr'] * CAT_MULT
                
                set_tpsl(sym, p['direction'], sl_px, cat_tp)
                open_positions[sym] = {
                    'direction': p['direction'], 'entry': fill_px, 'atr': p['atr'],
                    'sl_m': p['sl_m'], 'current_sl': sl_px, 'catastrophic_tp': cat_tp,
                    'best_price': fill_px, 'trail_active': False, 'lot': float(order.get('filled', po['lot'])),
                    'risk_usd': p['risk_usd']
                }
                del pending_orders[sym]
                
                log(f"{coin} Entry Filled @ {fill_px}", "EXEC")
                send_telegram(
                    f"<b>✅ ENTRY FILLED — {coin}</b>\n"
                    f"Setup : APEX {'🟢 LONG' if is_long else '🔴 SHORT'}\n"
                    f"Size  : <code>{open_positions[sym]['lot']}</code>\n"
                    f"Entry : <code>{fill_px:.4f}</code>\n"
                    f"SL    : <code>{sl_px:.4f}</code>\n"
                    f"Risk  : <code>${p['risk_usd']:.0f}</code>"
                )
            elif order.get('status') in ('canceled', 'rejected', 'expired'):
                del pending_orders[sym]
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
                records = res.get('result', {}).get('list', [])
                pnl = float(records[0].get('closedPnl', 0.0)) if records else 0.0
            except: pnl = 0.0
            
            record_pnl(pnl)
            outcome = 'WIN' if pnl > 0.5 else ('LOSS' if pnl < -0.5 else 'BREAKEVEN')
            emoji = '✅' if pnl > 0 else ('❌' if pnl < -0.5 else '➖')
            
            log(f"{sym.split('/')[0]} Closed | {outcome} | PnL: ${pnl:.2f}", "EXEC")
            send_telegram(
                f"{emoji} <b>TRADE CLOSED: {sym.split('/')[0]}</b>\n"
                f"Setup     : {'🟢 LONG' if pos['direction']=='LONG' else '🔴 SHORT'}\n"
                f"Size      : <code>{pos['lot']}</code>\n"
                f"Result    : <code>${pnl:.2f}</code> ({outcome})\n"
                f"Today PnL : <code>${get_today_pnl():.2f}</code>\n"
                f"Net Equity: <code>${equity_estimate:.2f}</code>"
            )

def manage_trailing_stops():
    for sym, pos in list(open_positions.items()):
        try:
            df = fetch_ohlcv(sym, '1m', 5)
            if df is None or len(df) < 1: continue
            
            is_long = pos['direction'] == 'LONG'
            live_close, live_hi, live_lo = float(df.iloc[-1]['close']), float(df.iloc[-1]['high']), float(df.iloc[-1]['low'])
            
            new_best = max(pos['best_price'], live_hi) if is_long else min(pos['best_price'], live_lo)
            pos['best_price'] = new_best
            dist = abs(new_best - pos['entry'])
            
            if not pos['trail_active'] and dist >= TP1_MULT * pos['atr']:
                pos['trail_active'] = True
                send_telegram(f"🛡️ <b>TRAIL ACTIVATED — {sym.split('/')[0]}</b>\nSL now trailing behind best price.")
                
            if not pos['trail_active']: continue
            
            raw_new_sl = (new_best - TRAIL_MULT * pos['atr']) if is_long else (new_best + TRAIL_MULT * pos['atr'])
            improved = (is_long and raw_new_sl > pos['current_sl']) or (not is_long and raw_new_sl < pos['current_sl'])
            
            if improved and ((is_long and raw_new_sl < live_close) or (not is_long and raw_new_sl > live_close)):
                fmt_sl = update_trailing_sl(sym, pos['direction'], raw_new_sl, pos['catastrophic_tp'])
                if fmt_sl: pos['current_sl'] = fmt_sl
        except: pass

def check_signals():
    log("Scanning coins...", "SCAN")
    if is_halted(): return
    
    if len(open_positions) + len(pending_orders) >= MAX_CONCURRENT: return
    sync_open_positions()
    
    for sym in SYMBOLS:
        if sym in open_positions or sym in pending_orders: continue
        
        result = compute_signals(sym)
        if not result: continue
        
        direction, entry_px, atr, sl_m, tr_m, risk_usd = result
        is_long = direction == 'LONG'
        sl_px = entry_px - sl_m * atr if is_long else entry_px + sl_m * atr
        cat_tp_px = entry_px + CAT_MULT * atr if is_long else entry_px - CAT_MULT * atr
        
        lot = calc_lot(entry_px, sl_px, risk_usd)
        if lot <= 0 or not fee_check(lot, entry_px, risk_usd): continue
        
        order_id, fmt_lot, fmt_entry = place_limit_entry(sym, direction, lot, entry_px, sl_px, cat_tp_px)
        if order_id:
            pending_orders[sym] = {
                'order_id': order_id, 'expires_at': utcnow().timestamp() + LIMIT_FILL_TIMEOUT,
                'entry_px': fmt_entry, 'lot': fmt_lot,
                'params': {'direction': direction, 'atr': atr, 'sl_m': sl_m, 'risk_usd': risk_usd}
            }
            log(f"Limit Order Placed: {sym.split('/')[0]} {direction}", "EXEC")
            send_telegram(
                f"<b>📋 SETUP DETECTED — {sym.split('/')[0]}</b>\n"
                f"Setup : APEX {'🟢 LONG' if is_long else '🔴 SHORT'}\n"
                f"Size  : <code>{fmt_lot}</code>\n"
                f"Limit : <code>{fmt_entry}</code>\n"
                f"SL    : <code>{sl_px:.4f}</code>\n"
                f"Risk  : <code>${risk_usd:.0f}</code>"
            )

def fast_management():
    log("Managing trades...", "MGMT")
    check_equity_floor()
    sync_open_positions()
    check_pending_orders()
    manage_trailing_stops()

# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP & SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    # Initial Telegram Boot Alert (Placed directly at start to guarantee execution)
    send_telegram(
        f"<b>🤯 APEX v7.2.8 PRO — CFT EDITION STARTED</b>\n"
        f"Coins   : ETH · SOL · LTC · ZEC\n"
        f"Orders  : Limit post-only\n"
        f"Risk    : ${BASE_RISK_USD:.0f} base\n"
        f"KS      : ${DAILY_KILL_SWITCH:.0f} | Floor: ${EQUITY_FLOOR_HALT:.0f}"
    )
    
    log("APEX SNIPER v7.2.8 PRO Booting...", "SYS")
    
    try:
        balance = exchange.fetch_balance()
        equity_estimate = float(balance.get('USDT', {}).get('total', CFT_ACCOUNT_SIZE))
        peak_equity = max(peak_equity, equity_estimate)
    except: pass

    schedule.every(1).minutes.do(fast_management)
    schedule.every(5).minutes.at(":00").do(check_signals)

    while True:
        schedule.run_pending()
        time.sleep(1)
