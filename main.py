import ccxt
import asyncio
import aiohttp
import os
import json
import pathlib
from datetime import datetime, date
from dotenv import load_dotenv
from aiohttp import web

# Load Environment Variables
load_dotenv()

# ── 🔥 PRECISION SNIPER WEBHOOK RECEIVER 🔥 ──
open_positions = {}
daily_pnl_tracker = {}
PNL_FILE = 'daily_pnl.json'
_tg_semaphore = asyncio.Semaphore(3)

# ── Credentials & Config ───────────────────────────────────────────
BYBIT_API_KEY      = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET   = os.getenv("BYBIT_API_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# 🛡️ Prop Firm Risk Management & Protection Filters
DAILY_KILL_SWITCH   = -155.0   
EQUITY_HARD_STOP    = -120.0   
BASE_RISK_PER_TRADE = 25.0    
MIN_SL_PCT          = 0.003   # 0.3% Minimum SL distance threshold to prevent fee bleeding

# Initialize Bybit REST Connection
exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY, 
    'secret': BYBIT_API_SECRET,
    'enableRateLimit': True, 
    'options': {'defaultType': 'swap'},
})
exchange.enable_demo_trading(True) 

# ── 🔥 STYLISH TERMINAL LOGS 🔥 ──
def stylish_log(action_type, symbol, message):
    now = datetime.now().strftime("%H:%M:%S")
    icons = {"WEBHOOK": "🌐", "EXECUTING": "⚡", "MANAGING": "🛡️", "CLOSED": "💰", "ERROR": "❌", "PROTECT": "🛑"}
    icon = icons.get(action_type, "🔹")
    print(f"[{now}] [{icon} {action_type.ljust(10)}] | {symbol.ljust(10)} | {message}")

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

# ── 🔥 EXCHANGE POSITION MANAGEMENT 🔥 ──
async def update_exchange_sl(symbol, new_sl):
    try:
        formatted_symbol = symbol.replace("/", "").split(":")[0]
        f_sl = str(float(exchange.price_to_precision(symbol, new_sl)))
        await asyncio.to_thread(
            exchange.privatePostV5PositionTradingStop, 
            {
                'category': 'linear', 
                'symbol': formatted_symbol, 
                'positionIdx': 0, 
                'stopLoss': f_sl,
                'tpslMode': 'Full'  
            }
        )
        return True
    except Exception as e:
        stylish_log("ERROR", symbol, f"Failed to modify SL on exchange: {e}")
        return False

# ── 🔥 TRADE EXECUTION ENGINE 🔥 ──
async def handle_signal_entry(data):
    action = data.get('action') 
    raw_ticker = data.get('ticker', '')
    
    clean_ticker = raw_ticker.split(':')[-1] 
    clean_ticker = clean_ticker.replace('.P', '').replace('USDT', '')
    symbol = f"{clean_ticker}/USDT:USDT"

    if is_kill_switch_active():
        stylish_log("PROTECT", symbol, "Signal skipped. Daily risk limit breached.")
        return

    direction = "LONG" if action == "buy" else "SHORT"
    side = "buy" if direction == "LONG" else "sell"
    
    try:
        entry_price = float(data['price'])
        sl = float(data['sl'])
        tp1 = float(data['tp1'])
        tp2 = float(data['tp2'])
        tp3 = float(data['tp3'])
        
        sl_dist = abs(entry_price - sl)
        if sl_dist == 0: return

        # 🛡️ MINIMUM STOP LOSS DISTANCE FILTER
        min_allowed_dist = entry_price * MIN_SL_PCT
        if sl_dist < min_allowed_dist:
            stylish_log("PROTECT", symbol, f"Signal skipped. SL distance ({sl_dist:.4f}) is under 0.3% threshold ({min_allowed_dist:.4f}). Preventing fee bleed.")
            skip_msg = f"⚠️ <b>SIGNAL REJECTED: {symbol}</b>\n" \
                       f"<b>Reason:</b> Micro Stop Loss Detected 🛑\n" \
                       f"<b>Current SL Dist:</b> {sl_dist:.4f}\n" \
                       f"<b>Minimum Allowed:</b> {min_allowed_dist:.4f} (0.3% of entry)\n" \
                       f"<i>Action: Skipped execution to block massive position sizing and extreme exchange taker fees.</i>"
            await send_telegram(skip_msg)
            return

        size = BASE_RISK_PER_TRADE / sl_dist
        f_size = float(exchange.amount_to_precision(symbol, size))
        f_sl = str(float(exchange.price_to_precision(symbol, sl)))
        f_tp3 = str(float(exchange.price_to_precision(symbol, tp3)))
        
        stylish_log("EXECUTING", symbol, f"Firing {direction} market order. Size: {f_size}")
        
        await asyncio.to_thread(
            exchange.create_order,
            symbol=symbol, type='market', side=side, amount=f_size, 
            params={'stopLoss': f_sl, 'takeProfit': f_tp3, 'positionIdx': 0, 'tpslMode': 'Full'}
        )
        
        open_positions[symbol] = {
            'direction': direction, 'entry': entry_price, 'qty': f_size,
            'sl_dist': sl_dist, 'sl': sl, 'current_sl': sl, 'be_px': entry_price,
            'tp1': tp1, 'tp2': tp2, 'tp3': tp3
        }

        icon = "🎯 🟢 LONG" if direction == "LONG" else "🎯 🔴 SHORT"
        msg = f"<b>🚀 POSITION OPENED: {symbol}</b>\n" \
              f"<b>Direction:</b> {icon}\n" \
              f"<b>Order Size:</b> {f_size} Contracts\n" \
              f"<b>Entry Price:</b> {entry_price:.4f}\n" \
              f"───────────────────\n" \
              f"<b>🚨 Initial SL:</b> {sl:.4f} ({(sl_dist/entry_price)*100:.2f}%)\n" \
              f"<b>🎯 Target TP1 (1R):</b> {tp1:.4f}\n" \
              f"<b>🎯 Target TP2 (2R):</b> {tp2:.4f}\n" \
              f"<b>🎯 Target TP3 (3R):</b> {tp3:.4f}\n" \
              f"───────────────────\n" \
              f"<b>🛡 shrink Strategy Rule:</b> SL automatically shifts to entry upon securing TP1."
        await send_telegram(msg)

    except Exception as e:
        stylish_log("ERROR", symbol, f"Execution failure: {e}")

# ── 🔥 TRADE MANAGEMENT EVENT HANDLER 🔥 ──
async def handle_management_event(data):
    event = data.get('event')
    raw_ticker = data.get('ticker', '')
    
    clean_ticker = raw_ticker.split(':')[-1] 
    clean_ticker = clean_ticker.replace('.P', '').replace('USDT', '')
    symbol = f"{clean_ticker}/USDT:USDT"

    if symbol not in open_positions:
        return

    pos = open_positions[symbol]

    if event == "tp1_hit":
        stylish_log("MANAGING", symbol, "Indicator confirmed TP1 hit. Protecting capital via Breakeven.")
        success = await update_exchange_sl(symbol, pos['entry'])
        if success:
            pos['current_sl'] = pos['entry'] 
        msg = f"🛡️ <b>RISK UPDATE: {symbol}</b>\n" \
              f"<b>Event Triggered:</b> 🎯 TP1 Reached (1R Locked)\n" \
              f"<b>Action Executed:</b> Stop Loss shifted to entry level (<b>{pos['entry']:.4f}</b>).\n" \
              f"<b>Current Risk Status:</b> Risk-Free Trade ✅"
        await send_telegram(msg)

    elif event == "tp2_hit":
        stylish_log("MANAGING", symbol, "Indicator confirmed TP2 hit. Securing profits at TP1.")
        success = await update_exchange_sl(symbol, pos['tp1'])
        if success:
            pos['current_sl'] = pos['tp1'] 
        msg = f"🛡️ <b>RISK UPDATE: {symbol}</b>\n" \
              f"<b>Event Triggered:</b> 🎯 TP2 Reached (2R Secured)\n" \
              f"<b>Action Executed:</b> Stop Loss trailed to take-profit 1 level (<b>{pos['tp1']:.4f}</b>).\n" \
              f"<b>Current Risk Status:</b> +1.00R Profit Guaranteed 🔒"
        await send_telegram(msg)

    elif event in ["tp3_hit", "sl_hit", "be_hit"]:
        if event == "tp3_hit":
            event_name = "TP3 Hit (Full Take Profit)"
            pnl_multiplier = 3.0
            icon = "🏆"
        elif pos['current_sl'] == pos['entry'] or event == "be_hit":
            event_name = "Breakeven Hit (Capital Protected)"
            pnl_multiplier = 0.0
            icon = "🛡️"
        elif pos['current_sl'] == pos['tp1']:
            event_name = "Trailed Stop Hit (Partial Take Profit)"
            pnl_multiplier = 1.0
            icon = "💰"
        else:
            event_name = "Initial Stop Loss Hit"
            pnl_multiplier = -1.0
            icon = "🛑"

        stylish_log("CLOSED", symbol, f"Trade completed via event: {event_name}")
        
        trade_pnl = pnl_multiplier * BASE_RISK_PER_TRADE
        daily_pnl_tracker[date.today()] = daily_pnl_tracker.get(date.today(), 0.0) + trade_pnl
        save_daily_pnl()
        
        msg = f"{icon} <b>POSITION CLOSED: {symbol}</b>\n" \
              f"<b>Reason for Exit:</b> {event_name}\n" \
              f"<b>Exit Parameter:</b> {pos['current_sl']:.4f}\n" \
              f"───────────────────\n" \
              f"<b>Realized Return:</b> {pnl_multiplier:+.2f}R\n" \
              f"<b>Net Dollar Impact:</b> {trade_pnl:+.2f} USD\n" \
              f"<b>Accumulated Daily PnL:</b> {daily_pnl_tracker[date.today()]:+.2f} USD"
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
    return web.Response(text="200 OK - BOT ALIVE")

# ── 🔥 CIRCUIT BREAKER LOOP 🔥 ──
async def equity_protection_loop():
    while True:
        try:
            if daily_pnl_tracker.get('equity_blown', False):
                await asyncio.sleep(60)
                continue
                
            pos_data = await asyncio.to_thread(exchange.fetch_positions)
            unrealized_pnl = sum(float(p.get('unrealisedPnl', 0.0)) for p in pos_data if float(p.get('contracts', 0)) > 0)
            realized_pnl = daily_pnl_tracker.get(date.today(), 0.0)
            live_equity = realized_pnl + unrealized_pnl
            
            if live_equity <= EQUITY_HARD_STOP:
                daily_pnl_tracker['equity_blown'] = True
                save_daily_pnl()
                stylish_log("PROTECT", "CORE", f"Live equity ({live_equity:.2f}) breached hard stop. Halting operations.")
                await send_telegram(f"🚨 <b>EQUITY CIRCUIT BREAKER TRIGGERED</b> 🚨\nTotal Daily Equity: ${live_equity:.2f}\nTrading operations halted safely.")
        except Exception as e:
            stylish_log("ERROR", "PROTECTION", f"Circuit loop error: {e}")
        await asyncio.sleep(15)

# ── 🔥 BOOT SEQUENCE 🔥 ──
async def main():
    load_daily_pnl()
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
    await send_telegram("🎯 <b>Precision Webhook Bot Online</b>\nListening for incoming TradingView signals 24/7.")
    
    await equity_protection_loop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
