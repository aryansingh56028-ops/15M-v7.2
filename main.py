import ccxt
import pandas as pd
import numpy as np
import requests
import schedule
import time
from datetime import datetime, timezone, date, timedelta

# ══════════════════════════════════════════════════════════════════════════════
#  APEX SNIPER v7.2.8 PRO  ——  CFT-PASSING EDITION
#  ─────────────────────────────────────────────────────────────────────────────
#  Universe  : ETH · SOL · LTC · ZEC  (4 most liquid — limit orders fill clean)
#  Order type: POST-ONLY LIMIT entries  (maker fee 0.020%, near-zero slippage)
#  Base risk : $20 / trade  →  $30 on House-Money days
#  Backtest  : +$4,375 / 15 mo | Max DD -$554 | P1 day ~23 | CFT ✅ PASS
#  Guardrails: KS=-$53 | Equity-Floor=-$300 | MaxConc=2 | FeeCap=55%
# ══════════════════════════════════════════════════════════════════════════════

# ── Credentials ───────────────────────────────────────────────────────────────
BYBIT_API_KEY      = "FOqGNCN6gRxu4bqMqF"
BYBIT_API_SECRET   = "YmSWYNkQbVXYiFU5v0G3y3R405VLREGu7icy"
TELEGRAM_BOT_TOKEN = "8734785957:AAGzU-KPRY4mzXARxyTpLSHGemFtJ7AEsUQ"
TELEGRAM_CHAT_ID   = "1932328527"
REPLIT_WEBHOOK_URL = "https://dcf37de3-95b1-4275-aad3-54160dffeae5-00-1dydpq6kaysyl.riker.replit.dev/api/webhook/trade"

# ── Indicator Params (100% Pine v7.2.8 PRO — settings screenshot confirmed) ──
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
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def log(msg: str, section: str = "BOT"):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}][{section}] {msg}", flush=True)

def log_divider(char: str = "─", width: int = 62):
    print(char * width, flush=True)

def log_header(cycle_type: str = "1-MIN"):
    dd     = current_drawdown()
    td_pnl = get_today_pnl()
    hm     = "🏦 HM ON" if house_money_arm() > 1.0 else "   HM off"
    ks     = "🔴 KS ACTIVE" if is_kill_switch_active() else "🟢 KS ok"
    fl     = "🛑 FLOOR HALT" if is_floor_halted() else "✅ Floor ok"
    log_divider("═")
    log(f"{'FAST MGMT' if cycle_type=='1-MIN' else 'SIGNAL SCAN'} CYCLE  |  "
        f"Eq: ${equity_estimate:.2f}  Peak: ${peak_equity:.2f}  DD: ${dd:.2f}", "STATUS")
    log(f"Today PnL: ${td_pnl:.2f}  |  {hm}  |  {ks}  |  {fl}", "STATUS")
    log(f"Open: {len(open_positions)}  |  Pending: {len(pending_orders)}  |  "
        f"Phase: {CURRENT_PHASE}  |  P1 {'✅' if p1_notified else '🔄'}  P2 {'✅' if p2_notified else '🔄'}", "STATUS")
    log_divider("─")

def log_open_positions_snapshot():
    if not open_positions:
        log("No open positions.", "POSITIONS")
        return
    log(f"{len(open_positions)} open position(s):", "POSITIONS")
    for sym, pos in open_positions.items():
        coin = sym.split('/')[0]
        try:
            ticker  = exchange.fetch_ticker(sym)
            live_px = float(ticker['last'])
        except Exception:
            live_px = pos['entry']
        is_long   = pos['direction'] == 'LONG'
        sl_dist   = pos['atr'] * pos['sl_m']
        r_dist    = (live_px - pos['entry']) if is_long else (pos['entry'] - live_px)
        unreal_pnl = (r_dist / sl_dist) * pos['risk_usd'] if sl_dist > 0 else 0.0
        sl_gap    = (live_px - pos['current_sl']) if is_long else (pos['current_sl'] - live_px)
        trail_tag = " [TRAILING]" if pos.get('trail_active') else " [awaiting trail]"
        pnl_tag   = f"+${unreal_pnl:.2f}" if unreal_pnl >= 0 else f"-${abs(unreal_pnl):.2f}"
        log(
            f"  {coin} {pos['direction']}  Entry:{pos['entry']:.4f}  Live:{live_px:.4f}  "
            f"UnrealPnL:{pnl_tag}  SL:{pos['current_sl']:.4f}  SLgap:{sl_gap:.4f}{trail_tag}",
            "POSITIONS"
        )

def log_pending_orders_snapshot():
    if not pending_orders:
        log("No pending limit orders.", "PENDING")
        return
    now = utcnow().timestamp()
    log(f"{len(pending_orders)} pending limit order(s):", "PENDING")
    for sym, po in pending_orders.items():
        remaining = max(0, po['expires_at'] - now)
        p = po['params']
        log(
            f"  {sym.split('/')[0]}  {p['direction']}  @ {po['entry_px']}  "
            f"Risk:${p['risk_usd']:.0f}  Expires in {remaining:.0f}s  ID:{po['order_id']}",
            "PENDING"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'
        }, timeout=10)
    except Exception as e:
        log(f"TG send failed: {e}", "TG")

def send_webhook(data: dict):
    if not REPLIT_WEBHOOK_URL:
        return
    try:
        requests.post(REPLIT_WEBHOOK_URL, json=data, timeout=5)
    except Exception as e:
        log(f"Webhook failed: {e}", "WEBHOOK")

def today() -> date:
    return date.today()

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def get_today_pnl() -> float:
    return daily_pnl_tracker.get(today(), 0.0)

def record_pnl(pnl_usd: float):
    global equity_estimate, peak_equity
    t = today()
    daily_pnl_tracker[t] = daily_pnl_tracker.get(t, 0.0) + pnl_usd
    equity_estimate      += pnl_usd
    if equity_estimate > peak_equity:
        peak_equity = equity_estimate

def current_drawdown() -> float:
    return equity_estimate - peak_equity

def is_kill_switch_active() -> bool:
    return get_today_pnl() <= DAILY_KILL_SWITCH

def is_floor_halted() -> bool:
    return today() in floor_halt_days

def is_halted() -> bool:
    return is_kill_switch_active() or is_floor_halted()

def house_money_arm() -> float:
    return HOUSE_MONEY_MULT if get_today_pnl() >= HOUSE_MONEY_THRESHOLD else 1.0


# ══════════════════════════════════════════════════════════════════════════════
#  INDICATOR MATH  (100% Pine v7.2.8 PRO equivalent)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(symbol: str, timeframe: str = '15m', limit: int = 300):
    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df   = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        for c in ['open', 'high', 'low', 'close']:
            df[c] = df[c].astype(float)
        return df
    except Exception as e:
        log(f"OHLCV fetch error [{symbol}]: {e}", "FETCH")
        return None

def rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1 / length, adjust=False).mean()

def calc_atr(df: pd.DataFrame, length: int) -> pd.Series:
    pc = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - pc).abs(),
        (df['low'] - pc).abs()
    ], axis=1).max(axis=1)
    return rma(tr, length)

def calc_wma(series: pd.Series, length: int) -> pd.Series:
    w = np.arange(1, length + 1)
    return series.rolling(length).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)

def algoalpha_baseline(df: pd.DataFrame) -> pd.Series:
    st_atr = calc_atr(df, ST_PERIOD)
    hl2    = (df['high'] + df['low']) / 2.0
    upper  = (hl2 + ST_FACTOR * st_atr).values
    lower  = (hl2 - ST_FACTOR * st_atr).values
    close  = df['close'].values
    n      = len(df)
    lo, up = np.zeros(n), np.zeros(n)
    lo[0], up[0] = lower[0], upper[0]
    for i in range(1, n):
        lo[i] = lower[i] if (lower[i] > lo[i-1] or close[i-1] < lo[i-1]) else lo[i-1]
        up[i] = upper[i] if (upper[i] < up[i-1] or close[i-1] > up[i-1]) else up[i-1]
    mid = (pd.Series(lo, index=df.index) + pd.Series(up, index=df.index)) / 2.0
    return calc_wma(mid, WMA_LENGTH).ewm(span=EMA_LENGTH, adjust=False).mean()

def calc_smc_structure(df: pd.DataFrame) -> pd.DataFrame:
    right  = FRACTAL_R
    window = FRACTAL_R * 2 + 1
    sh = pd.Series(np.where(
        df['high'].shift(right) == df['high'].rolling(window=window).max(),
        df['high'].shift(right), np.nan
    ), index=df.index).ffill()
    sl = pd.Series(np.where(
        df['low'].shift(right) == df['low'].rolling(window=window).min(),
        df['low'].shift(right), np.nan
    ), index=df.index).ffill()
    trend = pd.Series(np.nan, index=df.index)
    trend.loc[df['close'] > sh] =  1.0
    trend.loc[df['close'] < sl] = -1.0
    df['smc_trend'] = trend.ffill().fillna(0).astype(int)
    return df

def compute_signals(symbol: str):
    coin = symbol.split('/')[0]
    df   = fetch_ohlcv(symbol, '15m', 300)
    if df is None or len(df) < 150:
        log(f"  {coin}: insufficient data — skip", "SCAN")
        return None
    df['atr'] = calc_atr(df, ATR_PERIOD)
    df['tL']  = algoalpha_baseline(df)
    df        = calc_smc_structure(df)
    bar   = df.iloc[-2]
    atr   = float(bar['atr'])
    price = float(bar['close'])
    smc   = int(bar['smc_trend'])
    tL    = df['tL']
    if np.isnan(atr) or atr < price * 0.00005:
        log(f"  {coin}: ATR invalid ({atr:.6f}) — skip", "SCAN")
        return None
    algo_long  = (tL.iloc[-2] > tL.iloc[-3]) and (tL.iloc[-3] <= tL.iloc[-4])
    algo_short = (tL.iloc[-2] < tL.iloc[-3]) and (tL.iloc[-3] >= tL.iloc[-4])
    long_sig   = algo_short and (smc == -1)
    short_sig  = algo_long  and (smc ==  1)
    tl_dir  = "↑rising" if algo_long else ("↓falling" if algo_short else "→flat")
    smc_str = {1: "BULL", -1: "BEAR", 0: "NEUT"}.get(smc, "?")
    if not long_sig and not short_sig:
        log(f"  {coin}: no signal  tL={tl_dir}  SMC={smc_str}  ATR={atr:.4f}", "SCAN")
        return None
    sl_m, tp_m, tr_m, p1_r, p2_r = PER_SYMBOL_CONFIG[symbol]
    base_risk = p1_r if CURRENT_PHASE == 1 else p2_r
    risk_usd  = base_risk * house_money_arm()
    direction = 'LONG' if long_sig else 'SHORT'
    hm_tag    = " [HM]" if house_money_arm() > 1.0 else ""
    log(
        f"  {coin}: *** SIGNAL {direction} ***  tL={tl_dir}  SMC={smc_str}  "
        f"ATR={atr:.4f}  Price={price:.4f}  Risk=${risk_usd:.0f}{hm_tag}",
        "SCAN"
    )
    return direction, price, atr, sl_m, tr_m, risk_usd


# ══════════════════════════════════════════════════════════════════════════════
#  POSITION SIZING
# ══════════════════════════════════════════════════════════════════════════════

def calc_lot(entry: float, sl: float, risk_usd: float) -> float:
    sl_dist = abs(entry - sl)
    if sl_dist == 0:
        return 0.0
    lot = risk_usd / sl_dist
    if lot * entry > MAX_NOTIONAL_USD:
        lot = MAX_NOTIONAL_USD / entry
    return lot

def fee_check(lot: float, price: float, risk_usd: float) -> bool:
    est_fee = lot * price * (BYBIT_MAKER_FEE + BYBIT_TAKER_FEE)
    return est_fee <= risk_usd * FEE_CAP_FRAC


# ══════════════════════════════════════════════════════════════════════════════
#  ORDER EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def place_limit_entry(symbol: str, direction: str, lot: float,
                      entry_px: float, sl_px: float, cat_tp_px: float):
    side = 'buy' if direction == 'LONG' else 'sell'
    try:
        fmt_lot   = float(exchange.amount_to_precision(symbol, lot))
        fmt_entry = float(exchange.price_to_precision(symbol, entry_px))
        order     = exchange.create_order(
            symbol=symbol, type='limit', side=side, amount=fmt_lot, price=fmt_entry,
            params={'timeInForce': 'PostOnly'}
        )
        oid = order.get('id')
        log(f"Limit placed: {symbol.split('/')[0]} {direction} @ {fmt_entry}  lot:{fmt_lot}  ID:{oid}", "ORDER")
        return oid, fmt_lot, fmt_entry
    except Exception as e:
        log(f"Limit order FAILED [{symbol}]: {e}", "ORDER")
        send_telegram(f"❌ <b>Limit Order Failed — {symbol.split('/')[0]}</b>\n{e}")
        return None, None, None

def set_tpsl(symbol: str, direction: str, sl_px: float, cat_tp_px: float) -> bool:
    try:
        market_id  = exchange.market(symbol)['id']
        bybit_side = 'Buy' if direction == 'LONG' else 'Sell'
        fmt_sl     = float(exchange.price_to_precision(symbol, sl_px))
        fmt_tp     = float(exchange.price_to_precision(symbol, cat_tp_px))
        exchange.privatePostV5PositionTradingStop({
            'category': 'linear', 'symbol': market_id, 'side': bybit_side,
            'tpslMode': 'Full', 'takeProfit': str(fmt_tp),
            'stopLoss': str(fmt_sl), 'slOrderType': 'Market',
        })
        log(f"SL/TP set: {symbol.split('/')[0]}  SL={fmt_sl}  CatTP={fmt_tp}", "ORDER")
        return True
    except Exception as e:
        log(f"set_tpsl error [{symbol}]: {e}", "ORDER")
        return False

def update_trailing_sl(symbol: str, direction: str, new_sl: float, cat_tp: float) -> float | None:
    try:
        market_id  = exchange.market(symbol)['id']
        bybit_side = 'Buy' if direction == 'LONG' else 'Sell'
        fmt_sl     = float(exchange.price_to_precision(symbol, new_sl))
        fmt_tp     = float(exchange.price_to_precision(symbol, cat_tp))
        exchange.privatePostV5PositionTradingStop({
            'category': 'linear', 'symbol': market_id, 'side': bybit_side,
            'tpslMode': 'Full', 'takeProfit': str(fmt_tp),
            'stopLoss': str(fmt_sl), 'slOrderType': 'Market',
        })
        return fmt_sl
    except Exception as e:
        log(f"Trail SL update error [{symbol}]: {e}", "TRAIL")
        return None

def close_market(symbol: str, direction: str) -> float | None:
    side = 'sell' if direction == 'LONG' else 'buy'
    log(f"Force-closing {symbol.split('/')[0]} {direction} at market...", "CLOSE")
    try:
        pos_list = exchange.fetch_positions([symbol])
        if not pos_list:
            log(f"No live position for {symbol}", "CLOSE")
            return None
        size = float(pos_list[0].get('contracts', 0))
        if size <= 0:
            log(f"{symbol} already closed (size=0)", "CLOSE")
            return None
        exchange.create_order(symbol=symbol, type='market', side=side,
                              amount=size, params={'reduceOnly': True})
        time.sleep(2)
        res     = exchange.private_get_v5_position_closed_pnl(
            {'category': 'linear', 'symbol': exchange.market(symbol)['id'], 'limit': 1}
        )
        records = res.get('result', {}).get('list', [])
        pnl     = float(records[0].get('closedPnl', 0.0)) if records else 0.0
        log(f"{symbol.split('/')[0]} force-closed — PnL: ${pnl:.2f}", "CLOSE")
        return pnl
    except Exception as e:
        log(f"close_market error [{symbol}]: {e}", "CLOSE")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  EQUITY FLOOR HALT
# ══════════════════════════════════════════════════════════════════════════════

def check_equity_floor():
    dd = current_drawdown()
    if dd > EQUITY_FLOOR_HALT:
        return
    t = today()
    if t in floor_halt_days:
        return
    floor_halt_days.add(t)
    log(f"⚠️ EQUITY FLOOR TRIPPED — DD={dd:.2f}  limit={EQUITY_FLOOR_HALT:.0f}", "FLOOR")
    send_telegram(
        f"🛑 <b>EQUITY FLOOR TRIPPED</b>\n"
        f"DD from peak: <code>${dd:.2f}</code>  (limit: ${EQUITY_FLOOR_HALT:.0f})\n"
        f"Closing all positions + halting for the day."
    )
    log(f"Cancelling {len(pending_orders)} pending order(s)...", "FLOOR")
    for sym, po in list(pending_orders.items()):
        try:
            exchange.cancel_order(po['order_id'], sym)
            log(f"  Cancelled: {sym.split('/')[0]}", "FLOOR")
        except Exception as e:
            log(f"  Cancel error [{sym}]: {e}", "FLOOR")
    pending_orders.clear()
    total_pnl = 0.0
    log(f"Force-closing {len(open_positions)} open position(s)...", "FLOOR")
    for sym, pos in list(open_positions.items()):
        pnl = close_market(sym, pos['direction'])
        if pnl is not None:
            record_pnl(pnl)
            total_pnl += pnl
            send_webhook({'timestamp': utcnow().isoformat(), 'asset': sym,
                          'direction': pos['direction'], 'entry': pos['entry'],
                          'outcome': 'FLOOR_CLOSE', 'pnl': pnl, 'strategy': 'Apex CFT'})
    open_positions.clear()
    send_telegram(
        f"🛑 <b>ALL POSITIONS CLOSED — TRADING HALTED TODAY</b>\n"
        f"Closed PnL : <code>${total_pnl:.2f}</code>\n"
        f"Equity est : <code>${equity_estimate:.2f}</code>\n"
        f"DD from pk : <code>${current_drawdown():.2f}</code>\n"
        f"Resumes    : tomorrow 00:05 UTC"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  PENDING ORDER MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def check_pending_orders():
    if not pending_orders:
        return
    log(f"Checking {len(pending_orders)} pending limit order(s)...", "PENDING")
    now = utcnow().timestamp()
    for sym in list(pending_orders.keys()):
        po        = pending_orders[sym]
        remaining = po['expires_at'] - now
        coin      = sym.split('/')[0]
        if remaining <= 0:
            log(f"  {coin}: EXPIRED ({LIMIT_FILL_TIMEOUT}s elapsed) — cancelling", "PENDING")
            try:
                exchange.cancel_order(po['order_id'], sym)
                log(f"  {coin}: cancelled OK", "PENDING")
            except Exception as e:
                log(f"  {coin}: cancel error: {e}", "PENDING")
            del pending_orders[sym]
            send_telegram(f"⏱️ <b>Limit Expired — {coin}</b>\nNot filled in 15 min. Cancelled.")
            continue
        log(f"  {coin}: checking fill...  ({remaining:.0f}s remaining)", "PENDING")
        try:
            order = exchange.fetch_order(po['order_id'], sym)
        except Exception as e:
            log(f"  {coin}: fetch order error: {e}", "PENDING")
            continue
        status = order.get('status', '')
        log(f"  {coin}: status='{status}'", "PENDING")
        if status == 'closed':
            fill_px = float(order.get('average') or order.get('price') or po['entry_px'])
            p       = po['params']
            is_long = p['direction'] == 'LONG'
            sl_px   = fill_px - p['atr'] * p['sl_m'] if is_long else fill_px + p['atr'] * p['sl_m']
            cat_tp  = fill_px + p['atr'] * CAT_MULT  if is_long else fill_px - p['atr'] * CAT_MULT
            log(f"  {coin}: FILLED @ {fill_px:.4f}  SL={sl_px:.4f}  CatTP={cat_tp:.4f}", "PENDING")
            set_tpsl(sym, p['direction'], sl_px, cat_tp)
            open_positions[sym] = {
                'direction': p['direction'], 'entry': fill_px, 'atr': p['atr'],
                'sl_m': p['sl_m'], 'current_sl': sl_px, 'catastrophic_tp': cat_tp,
                'best_price': fill_px, 'trail_active': False, 'free_ride_alerted': False,
                'lot': float(order.get('filled', po['lot'])),
                'risk_usd': p['risk_usd'], 'hm_day': p['hm_day'],
            }
            del pending_orders[sym]
            est_fee = float(order.get('filled', po['lot'])) * fill_px * BYBIT_MAKER_FEE
            hm_tag  = ' 🏦 <i>HM day</i>' if p['hm_day'] else ''
            send_telegram(
                f"<b>✅ ENTRY FILLED — {coin}</b>{hm_tag}\n"
                f"{'🟢 LONG' if is_long else '🔴 SHORT'} @ <code>{fill_px:.4f}</code>\n"
                f"SL    : <code>{sl_px:.4f}</code>  ({p['sl_m']}×ATR)\n"
                f"CatTP : <code>{cat_tp:.2f}</code>  (10×ATR safety)\n"
                f"Risk  : <code>${p['risk_usd']:.0f}</code>  Fee: <code>${est_fee:.3f}</code>"
            )
        elif status in ('canceled', 'rejected', 'expired'):
            log(f"  {coin}: order {status} by Bybit — removing", "PENDING")
            del pending_orders[sym]


# ══════════════════════════════════════════════════════════════════════════════
#  POSITION SYNC
# ══════════════════════════════════════════════════════════════════════════════

def sync_open_positions():
    if not open_positions:
        return
    log(f"Syncing {len(open_positions)} position(s) with Bybit...", "SYNC")
    try:
        live      = exchange.fetch_positions()
        live_syms = {p['symbol'] for p in live if float(p.get('contracts', 0)) > 0}
        log(f"  Bybit live: {list(live_syms) or 'none'}", "SYNC")
    except Exception as e:
        log(f"  fetch_positions error: {e}", "SYNC")
        return
    closed = [s for s in list(open_positions.keys()) if s not in live_syms]
    if not closed:
        log("  All positions still live on Bybit.", "SYNC")
        return
    for sym in closed:
        pos = open_positions.pop(sym)
        log(f"  {sym.split('/')[0]}: closed on Bybit — fetching settled PnL...", "SYNC")
        try:
            res     = exchange.private_get_v5_position_closed_pnl(
                {'category': 'linear', 'symbol': exchange.market(sym)['id'], 'limit': 1}
            )
            records = res.get('result', {}).get('list', [])
            pnl     = float(records[0].get('closedPnl', 0.0)) if records else 0.0
        except Exception as e:
            log(f"  PnL fetch error [{sym}]: {e}", "SYNC")
            pnl = 0.0
        record_pnl(pnl)
        outcome = 'WIN' if pnl > 0.5 else ('LOSS' if pnl < -0.5 else 'BREAKEVEN')
        emoji   = '✅' if pnl > 0 else ('❌' if pnl < -0.5 else '➖')
        log(f"  {sym.split('/')[0]}: {outcome}  PnL=${pnl:.2f}  Today=${get_today_pnl():.2f}  Equity=${equity_estimate:.2f}", "SYNC")
        send_webhook({'timestamp': utcnow().isoformat(), 'asset': sym,
                      'direction': pos['direction'], 'entry': pos['entry'],
                      'outcome': outcome, 'pnl': pnl, 'strategy': 'Apex CFT'})
        send_telegram(
            f"{emoji} <b>TRADE CLOSED — {sym.split('/')[0]}</b>\n"
            f"{'🟢' if pos['direction']=='LONG' else '🔴'} {pos['direction']} | {outcome}\n"
            f"Net PnL  : <code>${pnl:.2f}</code>\n"
            f"Today    : <code>${get_today_pnl():.2f}</code>\n"
            f"Equity   : <code>${equity_estimate:.2f}</code>\n"
            f"DD / Peak: <code>${current_drawdown():.2f}</code> / <code>${peak_equity:.2f}</code>"
        )
        _check_milestones()
        check_equity_floor()

def _check_milestones():
    global p1_notified, p2_notified
    eq = equity_estimate
    if not p1_notified and eq >= CFT_P1_TARGET:
        p1_notified = True
        log(f"🏆 PHASE 1 HIT — ${eq:.2f}", "MILESTONE")
        send_telegram(f"🏆 <b>PHASE 1 TARGET HIT!</b>\nEquity: <code>${eq:.2f}</code> (+${eq-CFT_ACCOUNT_SIZE:.2f})\nSet CURRENT_PHASE = 2 and restart!")
    if not p2_notified and eq >= CFT_P2_TARGET:
        p2_notified = True
        log(f"🎉 PHASE 2 HIT — CHALLENGE PASSED — ${eq:.2f}", "MILESTONE")
        send_telegram(f"🎉 <b>PHASE 2 TARGET HIT — CHALLENGE PASSED!</b>\nEquity: <code>${eq:.2f}</code> (+${eq-CFT_ACCOUNT_SIZE:.2f})\nDD used: <code>${abs(current_drawdown()):.2f}</code> of ${CFT_MDD_LIMIT:.0f}")


# ══════════════════════════════════════════════════════════════════════════════
#  TRAILING STOP MANAGEMENT  (every 1 minute — catches reversals early)
# ══════════════════════════════════════════════════════════════════════════════

def manage_trailing_stops():
    if not open_positions:
        return
    log(f"Trailing stop check on {len(open_positions)} position(s)...", "TRAIL")
    for sym, pos in list(open_positions.items()):
        coin    = sym.split('/')[0]
        is_long = pos['direction'] == 'LONG'
        atr     = pos['atr']
        df = fetch_ohlcv(sym, '1m', 5)
        if df is None or len(df) < 1:
            log(f"  {coin}: 1m data unavailable — skip trail", "TRAIL")
            continue
        live       = df.iloc[-1]
        live_hi    = float(live['high'])
        live_lo    = float(live['low'])
        live_close = float(live['close'])
        new_best   = max(pos['best_price'], live_hi) if is_long else min(pos['best_price'], live_lo)
        pos['best_price'] = new_best
        entry   = pos['entry']
        cur_sl  = pos['current_sl']
        dist    = abs(new_best - entry)
        sl_dist = atr * pos['sl_m']
        unreal_r = ((live_close - entry) / sl_dist) if (is_long and sl_dist > 0) \
                   else ((entry - live_close) / sl_dist) if sl_dist > 0 else 0.0
        to_sl   = (live_close - cur_sl) if is_long else (cur_sl - live_close)
        log(
            f"  {coin}: live={live_close:.4f}  best={new_best:.4f}  SL={cur_sl:.4f}  "
            f"gap-to-SL={to_sl:.4f}  R={unreal_r:+.2f}  "
            f"dist={dist:.4f}/{TP1_MULT*atr:.4f}  trail={'ON' if pos['trail_active'] else 'off'}",
            "TRAIL"
        )
        if not pos['trail_active'] and dist >= TP1_MULT * atr:
            pos['trail_active']      = True
            pos['free_ride_alerted'] = True
            log(f"  {coin}: *** TRAIL ACTIVATED *** (dist {dist:.4f} ≥ trigger {TP1_MULT*atr:.4f})", "TRAIL")
            send_telegram(
                f"🛡️ <b>TRAIL ACTIVATED — {coin}</b>\n"
                f"Moved {dist:.4f} in our favour (≥1×ATR)\n"
                f"SL now trails at 0.10×ATR ({0.10*atr:.4f}) behind best price"
            )
        if not pos['trail_active']:
            log(f"  {coin}: need {TP1_MULT*atr - dist:.4f} more to activate trail", "TRAIL")
            continue
        raw_new_sl = (new_best - TRAIL_MULT * atr) if is_long else (new_best + TRAIL_MULT * atr)
        improved   = (is_long and raw_new_sl > cur_sl) or (not is_long and raw_new_sl < cur_sl)
        if not improved:
            log(f"  {coin}: new SL {raw_new_sl:.4f} not better than {cur_sl:.4f} — hold", "TRAIL")
            continue
        if (is_long and raw_new_sl >= live_close) or (not is_long and raw_new_sl <= live_close):
            log(f"  {coin}: new SL {raw_new_sl:.4f} past live price {live_close:.4f} — skip unsafe move", "TRAIL")
            continue
        fmt_sl = update_trailing_sl(sym, pos['direction'], raw_new_sl, pos['catastrophic_tp'])
        if fmt_sl:
            old_sl = pos['current_sl']
            pos['current_sl'] = fmt_sl
            log(f"  {coin}: SL trailed {old_sl:.4f} → {fmt_sl:.4f}  (gap to price={abs(live_close-fmt_sl):.4f})", "TRAIL")
            send_telegram(
                f"🔄 <b>SL Trailed — {coin}</b>\n"
                f"{'🟢' if is_long else '🔴'} {pos['direction']}\n"
                f"Old SL : <code>{old_sl:.4f}</code>\n"
                f"New SL : <code>{fmt_sl:.4f}</code>  (gap: {abs(live_close-fmt_sl):.4f})"
            )
        else:
            log(f"  {coin}: trail SL API call failed", "TRAIL")


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL ENGINE  (every 5 minutes at :00)
# ══════════════════════════════════════════════════════════════════════════════

def check_signals():
    log_header("SIGNAL")
    if is_halted():
        if is_floor_halted():
            log("HALTED — equity floor tripped today. No new entries.", "SCAN")
        else:
            log(f"KILL SWITCH — today PnL ${get_today_pnl():.2f} ≤ ${DAILY_KILL_SWITCH}. No new entries.", "SCAN")
        return
    available_slots = MAX_CONCURRENT - len(open_positions) - len(pending_orders)
    log(f"Available slots: {available_slots}/{MAX_CONCURRENT}", "SCAN")
    if available_slots <= 0:
        log("No slots available — skipping scan.", "SCAN")
        return
    log(f"Scanning: {[s.split('/')[0] for s in SYMBOLS]}", "SCAN")
    sync_open_positions()
    placed = 0
    for sym in SYMBOLS:
        if len(open_positions) + len(pending_orders) >= MAX_CONCURRENT:
            log("All slots filled — stopping scan.", "SCAN")
            break
        if sym in open_positions or sym in pending_orders:
            log(f"  {sym.split('/')[0]}: already open/pending — skip", "SCAN")
            continue
        result = compute_signals(sym)
        if result is None:
            continue
        direction, entry_px, atr, sl_m, tr_m, risk_usd = result
        is_long   = direction == 'LONG'
        sl_px     = entry_px - sl_m * atr if is_long else entry_px + sl_m * atr
        cat_tp_px = entry_px + CAT_MULT * atr if is_long else entry_px - CAT_MULT * atr
        lot       = calc_lot(entry_px, sl_px, risk_usd)
        if lot <= 0:
            log(f"  {sym.split('/')[0]}: lot=0 — skip", "SCAN")
            continue
        est_fee = lot * entry_px * (BYBIT_MAKER_FEE + BYBIT_TAKER_FEE)
        if not fee_check(lot, entry_px, risk_usd):
            log(f"  {sym.split('/')[0]}: fee cap exceeded ${est_fee:.3f} > {FEE_CAP_FRAC*100:.0f}% of ${risk_usd:.0f} — skip", "SCAN")
            continue
        log(
            f"  {sym.split('/')[0]}: placing limit {direction}  entry={entry_px:.4f}  "
            f"SL={sl_px:.4f}  lot={lot:.6f}  est_fee=${est_fee:.3f}  risk=${risk_usd:.0f}",
            "SCAN"
        )
        order_id, fmt_lot, fmt_entry = place_limit_entry(sym, direction, lot, entry_px, sl_px, cat_tp_px)
        if not order_id:
            continue
        hm_active = house_money_arm() > 1.0
        pending_orders[sym] = {
            'order_id': order_id, 'expires_at': utcnow().timestamp() + LIMIT_FILL_TIMEOUT,
            'entry_px': fmt_entry, 'lot': fmt_lot,
            'params': {'direction': direction, 'atr': atr, 'sl_m': sl_m,
                       'risk_usd': risk_usd, 'hm_day': hm_active},
        }
        placed += 1
        hm_tag = ' 🏦 HM' if hm_active else ''
        send_telegram(
            f"<b>📋 LIMIT ORDER PLACED — {sym.split('/')[0]}</b>{hm_tag}\n"
            f"{'🟢 LONG' if is_long else '🔴 SHORT'} @ <code>{fmt_entry}</code>  (post-only)\n"
            f"SL    : <code>{sl_px:.4f}</code>  ({sl_m}×ATR)\n"
            f"CatTP : <code>{cat_tp_px:.2f}</code>  (10×ATR)\n"
            f"Risk  : <code>${risk_usd:.0f}</code>  Est.fee: <code>${est_fee:.3f}</code>\n"
            f"Expires in 15 min if not filled"
        )
    log(f"Signal scan complete — {placed} new order(s) placed.", "SCAN")


# ══════════════════════════════════════════════════════════════════════════════
#  FAST MANAGEMENT LOOP  (every 1 minute)
# ══════════════════════════════════════════════════════════════════════════════

def fast_management():
    log_header("1-MIN")
    log_open_positions_snapshot()
    log_pending_orders_snapshot()
    log_divider("·")
    log("Step 1/4 — equity floor check...", "MGMT")
    check_equity_floor()
    log("Step 2/4 — position sync with Bybit...", "MGMT")
    sync_open_positions()
    log("Step 3/4 — pending limit order check...", "MGMT")
    check_pending_orders()
    log("Step 4/4 — trailing stop management...", "MGMT")
    manage_trailing_stops()
    log("Cycle complete.", "MGMT")
    log_divider("═")


# ══════════════════════════════════════════════════════════════════════════════
#  DAILY REPORT + RESET
# ══════════════════════════════════════════════════════════════════════════════

def daily_report():
    yesterday = date.today() - timedelta(days=1)
    yd_pnl    = daily_pnl_tracker.get(yesterday, 0.0)
    log(f"Daily report — {yesterday}  PnL=${yd_pnl:.2f}  Equity=${equity_estimate:.2f}", "DAILY")
    send_telegram(
        f"📅 <b>DAILY REPORT — {yesterday}</b>\n"
        f"Yesterday PnL  : <code>${yd_pnl:.2f}</code>\n"
        f"Account equity : <code>${equity_estimate:.2f}</code>\n"
        f"Peak equity    : <code>${peak_equity:.2f}</code>\n"
        f"DD from peak   : <code>${current_drawdown():.2f}</code>  (CFT limit: -${CFT_MDD_LIMIT:.0f})\n"
        f"Floor halt used: {'Yes ⚠️' if yesterday in floor_halt_days else 'No ✅'}\n"
        f"Open positions : {len(open_positions)}\n"
        f"Phase: {CURRENT_PHASE}  |  P1 {'✅' if p1_notified else '🔄'}  P2 {'✅' if p2_notified else '🔄'}"
    )

def daily_reset():
    log("Daily reset — new day started, KS cleared.", "DAILY")
    send_telegram("📅 <b>New day started</b> — kill-switch reset. Trading resumes.")


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP EQUITY SYNC
# ══════════════════════════════════════════════════════════════════════════════

def startup_sync():
    global equity_estimate, peak_equity
    log("Fetching live Bybit equity...", "STARTUP")
    try:
        balance = exchange.fetch_balance()
        usdt    = float(balance.get('USDT', {}).get('total', CFT_ACCOUNT_SIZE))
        equity_estimate = usdt
        peak_equity     = max(peak_equity, usdt)
        log(f"Bybit equity synced: ${usdt:.2f}  (peak set to ${peak_equity:.2f})", "STARTUP")
    except Exception as e:
        log(f"Equity sync failed: {e} — using default ${CFT_ACCOUNT_SIZE}", "STARTUP")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    log_divider("═")
    log("APEX SNIPER v7.2.8 PRO — CFT EDITION", "STARTUP")
    log(f"Symbols     : {[s.split('/')[0] for s in SYMBOLS]}", "STARTUP")
    log(f"Risk        : ${BASE_RISK_USD} base / ${BASE_RISK_USD*HOUSE_MONEY_MULT:.0f} HM (daily PnL ≥ +${HOUSE_MONEY_THRESHOLD:.0f})", "STARTUP")
    log(f"Kill switch : daily PnL ≤ ${DAILY_KILL_SWITCH}", "STARTUP")
    log(f"Equity floor: DD ≤ ${EQUITY_FLOOR_HALT} → closes all + halts", "STARTUP")
    log(f"Max conc    : {MAX_CONCURRENT}  |  Order type: POST-ONLY LIMIT", "STARTUP")
    log(f"Maker fee   : {BYBIT_MAKER_FEE*100:.3f}%  |  Taker fee: {BYBIT_TAKER_FEE*100:.3f}%", "STARTUP")
    log(f"Phase       : {CURRENT_PHASE}  |  P1 target: +${CFT_P1_TARGET-CFT_ACCOUNT_SIZE:.0f}  P2 target: +${CFT_P2_TARGET-CFT_ACCOUNT_SIZE:.0f}", "STARTUP")
    log(f"Demo mode   : {exchange.demo}", "STARTUP")
    log_divider("═")

    startup_sync()

    send_telegram(
        f"<b>🤯 APEX v7.2.8 PRO — CFT EDITION STARTED</b>\n"
        f"Coins   : ETH · SOL · LTC · ZEC\n"
        f"Orders  : Limit post-only (maker {BYBIT_MAKER_FEE*100:.3f}%)\n"
        f"Risk    : ${BASE_RISK_USD:.0f} base → ${BASE_RISK_USD*HOUSE_MONEY_MULT:.0f} on HM days\n"
        f"KS      : ${DAILY_KILL_SWITCH:.0f}  |  Floor: ${EQUITY_FLOOR_HALT:.0f}  |  MaxConc: {MAX_CONCURRENT}\n"
        f"Equity  : <code>${equity_estimate:.2f}</code>\n"
        f"Phase   : {CURRENT_PHASE}  |  Demo: {exchange.demo}"
    )

    schedule.every(1).minutes.do(fast_management)
    schedule.every(5).minutes.at(":00").do(check_signals)
    schedule.every().day.at("00:01").do(daily_report)
    schedule.every().day.at("00:05").do(daily_reset)

    log("Scheduler running — fast_management=1min  check_signals=5min(:00)", "STARTUP")
    log_divider("═")

    while True:
        schedule.run_pending()
        time.sleep(1)
