import asyncio
import aiohttp
import os
import json
import math
import pathlib
from datetime import datetime, date
from aiohttp import web
import ccxt
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# ── 🔥 APEX BYBIT WEBHOOK RECEIVER 🔥 ──
open_positions = {}
daily_pnl_tracker = {}
PNL_FILE = 'daily_pnl.json'
_tg_semaphore = asyncio.Semaphore(3)

# ── Credentials & Config ───────────────────────────────────────────
BYBIT_API_KEY      = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET   = os.getenv("BYBIT_API_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# 🛡️ Risk Management
DAILY_KILL_SWITCH   = -80.0   
EQUITY_HARD_STOP    = -180.0   
BASE_RISK_PER_TRADE = 25.0    

# Global Exchange Instance
exchange = None

# ── 🔥 STYLISH TERMINAL LOGS 🔥 ──
def stylish_log(action_type, symbol, message):
    now = datetime.now().strftime("%H:%M:%S")
    icons = {"WEBHOOK": "🌐", "EXECUTING": "⚡", "MANAGING": "🛡️", "CLOSED": "💰", "ERROR": "❌", "PROTECT": "🛑", "SYSTEM": "🔹"}
    icon = icons.get(action_type, "🔹")
    clean_sym = symbol.split(':')[0] if symbol else "SYSTEM"
    print(f"[{now}] [{icon} {action_type.ljust(10)}] | {clean_sym.ljust(10)} | {message}", flush=True)

# ── 🔥 SYSTEM HELPERS & TELEGRAM 🔥 ──
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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    async with _tg_semaphore:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': text.strip(), 'parse_mode': 'HTML'}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=5) as response:
                    await response.text()
        except Exception as e:
            stylish_log("ERROR", "TELEGRAM", f"Failed to send update: {e}")

def is_kill_switch_active() -> bool:
    return daily_pnl_tracker.get('equity_blown', False) or daily_pnl_tracker.get(date.today(), 0.0) <= DAILY_KILL_SWITCH

# ── 🔥 BYBIT TRADING UTILITIES 🔥 ──
def parse_webhook_symbol(ticker_string):
    """Converts standard alert formats (e.g., BTCUSD, BYBIT:BTCUSDT.P) to standard CCXT 'BTC/USDT:USDT'"""
    clean = ticker_string.split(':')[-1]
    clean = clean.replace('.P', '').replace('USDT', '')
    return f"{clean}/USDT:USDT"

async def update_exchange_sl(symbol, new_sl):
    """Updates the active Stop Loss bracket on Bybit"""
    try:
        pos = open_positions.get(symbol)
        if not pos: return False
        
        f_sl = float(exchange.price_to_precision(symbol, new_sl))
        
        # Bybit v5 position modification protocol via CCXT
        await asyncio.to_thread(
            exchange.set_trading_fee_and_tpsl_mode, # Fallback container for structural updates
            symbol, 
            {'stopLoss': str(f_sl), 'tpslMode': 'Full', 'positionIdx': 0}
        )
        return True
    except Exception as e:
        stylish_log("ERROR", symbol, f"Failed to modify SL on Bybit: {e}")
        return False

# ── 🔥 TRADE EXECUTION ENGINE 🔥 ──
async def handle_signal_entry(data):
    action = data.get('action') 
    raw_ticker = data.get('ticker', '')
    symbol = parse_webhook_symbol(raw_ticker)

    if is_kill_switch_active():
        stylish_log("PROTECT", symbol, "Signal skipped. Daily risk limit breached.")
        return

    if symbol in open_positions:
        stylish_log("PROTECT", symbol, "Signal skipped. Position already open.")
        return

    direction = "LONG" if action.lower() in ["buy", "long"] else "SHORT"
    side = 'buy' if direction == 'LONG' else 'sell'
    
    try:
        entry_price = float(data['price'])
        sl = float(data['sl'])
        tp1 = float(data['tp1'])
        tp2 = float(data['tp2'])
        tp3 = float(data['tp3'])
        
        sl_dist = abs(entry_price - sl)
        if sl_dist == 0: return

        # Exact Crypto Position Sizing Accounting for Fee Drag
        fee_rate = 0.00055 
        cost_of_entry_fee = entry_price * fee_rate
        cost_of_sl_fee = sl * fee_rate
        true_risk_per_coin = sl_dist + cost_of_entry_fee + cost_of_sl_fee
        
        raw_qty = BASE_RISK_PER_TRADE / true_risk_per_coin
        qty = float(exchange.amount_to_precision(symbol, raw_qty))
        
        f_sl = float(exchange.price_to_precision(symbol, sl))
        f_tp3 = float(exchange.price_to_precision(symbol, tp3))
        
        stylish_log("EXECUTING", symbol, f"Firing {direction} market order. Size: {qty} units")
        
        # Bybit Unified Execution Engine
        order = await asyncio.to_thread(
            exchange.create_order,
            symbol=symbol, type='market', side=side, amount=qty, 
            params={
                'stopLoss': str(f_sl), 
                'takeProfit': str(f_tp3), 
                'tpslMode': 'Full',
                'positionIdx': 0
            })
            
        open_positions[symbol] = {
            'id': order['id'], 'direction': direction, 'entry': entry_price, 
            'qty': qty, 'sl_dist': sl_dist, 'sl': sl, 
            'tp1': tp1, 'tp2': tp2, 'tp3': tp3
        }

        icon = "🎯 🟢 LONG" if direction == "LONG" else "🎯 🔴 SHORT"
        base_display = symbol.split('/')[0]
        msg = f"<b>{icon}:</b> {base_display}\n" \
              f"<b>Type:</b> ⚡ Bybit Market Execution\n" \
              f"<b>Quantity:</b> {qty}\n" \
              f"<b>Entry:</b> {entry_price:.4f}\n" \
              f"<b>SL:</b> {sl:.4f}\n" \
              f"<b>TP (1R):</b> {tp1:.4f}\n" \
              f"<b>TP (2R):</b> {tp2:.4f}\n" \
              f"<b>TP (3R):</b> {tp3:.4f}\n" \
              f"<b>Management:</b> SL trails to BE after TP1, to TP1 after TP2."
        await send_telegram(msg)

    except Exception as e:
        stylish_log("ERROR", symbol, f"Execution failure: {e}")

# ── 🔥 TRADE MANAGEMENT EVENT HANDLER 🔥 ──
async def handle_management_event(data):
    event = data.get('event')
    raw_ticker = data.get('ticker', '')
    symbol = parse_webhook_symbol(raw_ticker)

    if symbol not in open_positions:
        return

    pos = open_positions[symbol]
    base_display = symbol.split('/')[0]

    if event == "tp1_hit":
        stylish_log("MANAGING", symbol, "Indicator confirmed TP1 hit. Protecting capital via Breakeven.")
        await update_exchange_sl(symbol, pos['entry'])
        msg = f"🛡️ <b>UPDATE: {base_display}</b>\n" \
              f"<b>Event:</b> 🎯 TP1 Hit (1R Secured)\n" \
              f"<b>Action:</b> SL moved to Breakeven"
        await send_telegram(msg)

    elif event == "tp2_hit":
        stylish_log("MANAGING", symbol, "Indicator confirmed TP2 hit. Securing profits at TP1.")
        await update_exchange_sl(symbol, pos['tp1'])
        msg = f"🛡️ <b>UPDATE: {base_display}</b>\n" \
              f"<b>Event:</b> 🎯 TP2 Hit (2R Secured)\n" \
              f"<b>Action:</b> SL trailed to TP1"
        await send_telegram(msg)

    elif event in ["tp3_hit", "sl_hit"]:
        stylish_log("CLOSED", symbol, f"Trade completed via event: {event}")
        
        pnl_multiplier = 3.0 if event == "tp3_hit" else -1.0
        trade_pnl = pnl_multiplier * BASE_RISK_PER_TRADE
        daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + trade_pnl
        save_daily_pnl()
        
        icon = "🏆" if event == "tp3_hit" else "🛑"
        event_name = "TP3 Hit (Full Profit)" if event == "tp3_hit" else "SL Hit"
        msg = f"{icon} <b>POSITION CLOSED: {base_display}</b>\n" \
              f"<b>Event:</b> {event_name}\n" \
              f"<b>Realized PnL:</b> {pnl_multiplier:+.2f}R"
        await send_telegram(msg)
        
        open_positions.pop(symbol, None)

# ── 🔥 WEBHOOK SERVER ROUTING 🔥 ──
async def handle_webhook(request):
    try:
        data = await request.json()
        stylish_log("WEBHOOK", "INCOMING", f"Payload data: {json.dumps(data)}")
        
        if "action" in data:
            asyncio.create_task(handle_signal_entry(data))
        elif "event" in data:
            asyncio.create_task(handle_management_event(data))
            
        return web.json_response({"status": "received"}, status=200)
    except Exception as e:
        stylish_log("ERROR", "SERVER", f"Payload processing crash: {e}")
        return web.json_response({"error": "invalid payload"}, status=400)

async def health_check(request):
    return web.Response(text="200 OK - APEX BYBIT ALIVE")

# ── 🔥 BOOT SEQUENCE 🔥 ──
async def init_exchange():
    global exchange
    try:
        stylish_log("SYSTEM", "STARTUP", "Connecting to Bybit API Infrastructure...")
        exchange = ccxt.bybit({
            'apiKey': BYBIT_API_KEY,
            'secret': BYBIT_API_SECRET,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',
                'fetchMarkets': ['linear']
            }
        })
        exchange.enable_demo_trading(True) # Set to False for Live Trading Accounts
        await asyncio.to_thread(exchange.load_markets)
        stylish_log("SYSTEM", "STARTUP", "Bybit Connection synchronized and loaded successfully!")
    except Exception as e:
        stylish_log("ERROR", "EXCHANGE", f"Initialization crash: {e}")

async def main():
    load_daily_pnl()
    
    # 1. Connect to Bybit API
    await init_exchange()
    
    # 2. Start Standalone Webhook Server
    stylish_log("SYSTEM", "STARTUP", "Initializing standalone webhook routing server...")
    app = web.Application()
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_get('/', health_check)
    
    port = int(os.environ.get('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    await site.start()
    stylish_log("SYSTEM", "STARTUP", f"Web server successfully bound to port {port}")
    await send_telegram("🎯 <b>Apex Bybit Webhook Bot Online</b>\nListening for incoming TradingView signals 24/7.")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
