import os
import time
import json
import asyncio
import websockets
import requests
import pandas as pd
import numpy as np
import html
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
from aiohttp import web

# --- STRATEGY PARAMETERS ---
SYMBOL = "frxEURUSD"
STAKE = 10.0
ROLLING_WINDOW = 20
MULTIPLIER = 3.0
MAX_MULTIPLIER = 99.0
RSI_PERIOD = 7
RSI_OB = 75
RSI_OS = 25
DURATION = 15
DURATION_UNIT = "m"

# --- RISK & FILTER SETTINGS ---
MIN_PAYOUT_PCT = 70.0
USE_TIME_FILTER = True
BLACKOUT_START_HOUR = 6  # 06:00 GMT
BLACKOUT_END_HOUR = 8    # 08:00 GMT
BLOCK_THURSDAYS = True   # Disable trading on Thursdays

# --- GLOBAL STATE FOR DASHBOARD ---
bot_state = {
    "total_trades": 0,
    "trades_won": 0,
    "trades_lost": 0,
    "starting_balance": 0.0,
    "current_balance": 0.0,
    "total_profit": 0.0,
    "last_signal": "None",
    "last_signal_time": "Never",
    "last_candle_rsi": 0.0,
    "last_candle_body": 0.0,
    "last_candle_avg_body": 0.0,
    "last_update": "Never",
    "last_trade_epoch": 0,
    "pending_trade_details": {}
}
app_logs = deque(maxlen=100)
trade_history = deque(maxlen=50)

def log(msg):
    now = datetime.now()
    time_str = now.strftime('%Y-%m-%d %H:%M:%S')
    date_str = now.strftime('%Y-%m-%d')
    formatted = f"[{time_str}] {msg}"
    print(formatted)
    app_logs.appendleft(formatted)
    
    # Save to daily log file
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/bot_log_{date_str}.txt", "a", encoding="utf-8") as f:
        f.write(formatted + "\n")

def calculate_rsi(prices, periods=14):
    if len(prices) < periods + 1:
        return np.zeros_like(prices)
    delta = np.diff(prices)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    rsi = np.zeros_like(prices)
    rsi[:periods] = 50.0
    
    for i in range(periods, len(prices)):
        avg_gain = np.mean(gain[i-periods:i])
        avg_loss = np.mean(loss[i-periods:i])
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsi

async def get_otp_ws_url():
    token = os.environ.get("DERIV_API_TOKEN")
    account = os.environ.get("DERIV_ACCOUNT_ID")
    app_id = os.environ.get("DERIV_APP_ID")
    
    url = f"https://api.derivws.com/trading/v1/options/accounts/{account}/otp"
    headers = {
        "Authorization": f"Bearer {token}",
        "Deriv-App-ID": app_id,
        "Content-Type": "application/json"
    }
    
    res = requests.post(url, headers=headers)
    if res.status_code == 200:
        return res.json().get("data", {}).get("url")
    else:
        log(f"Failed to get OTP WS URL. Status: {res.status_code}. Response: {res.text}")
        return None

async def fetch_balance():
    ws_url = await get_otp_ws_url()
    if not ws_url:
        return None
    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"balance": 1}))
            res = json.loads(await ws.recv())
            if "balance" in res:
                return float(res["balance"]["balance"])
    except Exception as e:
        log(f"Error fetching balance: {e}")
    return None

async def monitor_contract(contract_id, trade_info=None):
    ws_url = await get_otp_ws_url()
    if not ws_url:
        return
    log(f"Monitoring contract {contract_id} for profit/loss...")
    try:
        async with websockets.connect(ws_url) as ws:
            req = {"proposal_open_contract": 1, "contract_id": contract_id, "subscribe": 1}
            await ws.send(json.dumps(req))
            while True:
                res = json.loads(await ws.recv())
                if "error" in res:
                    log(f"Contract Monitor Error: {res['error']['message']}")
                    break
                contract = res.get("proposal_open_contract", {})
                if contract.get("is_sold"):
                    profit = float(contract.get("profit", 0.0))
                    status = contract.get("status")
                    log(f"Contract {contract_id} closed! Status: {status}, Profit: ${profit:.2f}")
                    
                    bot_state["total_profit"] += profit
                    if profit > 0:
                        bot_state["trades_won"] += 1
                    else:
                        bot_state["trades_lost"] += 1
                        
                    bal = await fetch_balance()
                    if bal is not None:
                        bot_state["current_balance"] = bal
                        
                    if trade_info:
                        trade_info["profit"] = profit
                        trade_info["status"] = status
                        trade_info["balance_after"] = bot_state["current_balance"]
                    break
    except Exception as e:
        log(f"Error monitoring contract {contract_id}: {e}")

async def execute_trade(contract_type):
    ws_url = await get_otp_ws_url()
    if not ws_url:
        return
        
    log(f"Connecting to authenticated WebSocket for trade execution...")
    async with websockets.connect(ws_url) as ws:
        # Step 1: Request Proposal
        proposal_req = {
            "proposal": 1,
            "amount": STAKE,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "underlying_symbol": SYMBOL,
            "duration": DURATION,
            "duration_unit": DURATION_UNIT
        }
        await ws.send(json.dumps(proposal_req))
        res = json.loads(await ws.recv())
        
        if "error" in res:
            log(f"Error fetching proposal: {res['error']['message']}")
            return
            
        proposal = res.get("proposal", {})
        proposal_id = proposal.get("id")
        ask_price = proposal.get("ask_price")
        payout = proposal.get("payout")
        log(f"Obtained Quote -> Price: {ask_price}, Payout: {payout}, Proposal ID: {proposal_id}")
        
        if ask_price and payout:
            payout_pct = ((payout - ask_price) / ask_price) * 100
            if payout_pct < MIN_PAYOUT_PCT:
                log(f"⚠️ Trade Rejected! Payout is only {payout_pct:.1f}% (Below {MIN_PAYOUT_PCT}% threshold).")
                return
        
        # Step 2: Buy
        buy_req = {
            "buy": proposal_id,
            "price": ask_price
        }
        
        log(f"Sending BUY request...")
        await ws.send(json.dumps(buy_req))
        buy_res = json.loads(await ws.recv())
        
        if "error" in buy_res:
            log(f"Trade Execution Failed: {buy_res['error']['message']}")
        else:
            contract_id = buy_res['buy']['contract_id']
            log(f"Trade Executed Successfully! Contract ID: {contract_id}")
            bot_state["total_trades"] += 1
            
            trade_info = {
                "contract_id": contract_id,
                "type": contract_type,
                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "details": bot_state.get("pending_trade_details", {}).copy(),
                "balance_before": bot_state["current_balance"],
                "profit": 0.0,
                "balance_after": 0.0,
                "status": "OPEN"
            }
            trade_history.appendleft(trade_info)
            asyncio.create_task(monitor_contract(contract_id, trade_info))

async def fetch_recent_candles():
    uri = "wss://api.derivws.com/trading/v1/options/ws/public"
    async with websockets.connect(uri) as ws:
        req = {
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": ROLLING_WINDOW + RSI_PERIOD + 2, 
            "end": "latest",
            "style": "candles",
            "granularity": 900 
        }
        await ws.send(json.dumps(req))
        response = json.loads(await ws.recv())
        return response.get("candles", [])

def check_for_signal(candles):
    if len(candles) < ROLLING_WINDOW + RSI_PERIOD:
        log("Not enough candles fetched to calculate strategy.")
        return None
        
    df = pd.DataFrame(candles)
    
    current_epoch = int(time.time())
    current_boundary_epoch = current_epoch - (current_epoch % 900)
    
    if df.iloc[-1]['epoch'] >= current_boundary_epoch:
        df = df.iloc[:-1].copy()
    else:
        df = df.copy()
    
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    
    df['returns'] = df['close'] - df['open']
    df['body_size'] = abs(df['returns'])
    df['direction'] = np.where(df['returns'] > 0, 1, -1)
    
    df['avg_body_size'] = df['body_size'].rolling(window=ROLLING_WINDOW).mean().shift(1)
    df['rsi'] = calculate_rsi(df['close'].values, RSI_PERIOD)
    
    last_closed = df.iloc[-1]
    
    # Time Filter Check
    candle_time = pd.to_datetime(last_closed['epoch'], unit='s')
    current_hour = candle_time.hour
    
    if USE_TIME_FILTER and (BLACKOUT_START_HOUR <= current_hour < BLACKOUT_END_HOUR):
        bot_state["last_update"] = f"{candle_time} (Blackout Period)"
        log(f"⏸️ Signal ignored. {current_hour}:00 GMT is within the {BLACKOUT_START_HOUR}:00 - {BLACKOUT_END_HOUR}:00 blackout window.")
        return None
        
    if BLOCK_THURSDAYS and candle_time.dayofweek == 3:
        bot_state["last_update"] = f"{candle_time} (Thursday Block)"
        log("⏸️ Signal ignored. Trading is disabled on Thursdays to avoid macro volatility traps.")
        return None
    
    if pd.isna(last_closed['avg_body_size']) or pd.isna(last_closed['rsi']):
        return None
        
    multiplier_val = last_closed['body_size'] / last_closed['avg_body_size'] if last_closed['avg_body_size'] > 0 else 0
    is_breakout = (multiplier_val > MULTIPLIER) and (multiplier_val < MAX_MULTIPLIER)
    rsi = last_closed['rsi']
    direction = last_closed['direction']
    
    # Update state for dashboard
    bot_state["last_candle_rsi"] = float(rsi)
    bot_state["last_candle_body"] = float(last_closed['body_size'])
    bot_state["last_candle_avg_body"] = float(last_closed['avg_body_size'])
    bot_state["last_update"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    candle_time = pd.to_datetime(last_closed['epoch'], unit='s')
    log(f"Analyzed 15m candle (Epoch: {candle_time}) -> Body: {last_closed['body_size']:.5f}, Avg: {last_closed['avg_body_size']:.5f}, Mult: {multiplier_val:.2f}x, RSI: {rsi:.2f}")

    if is_breakout:
        if direction == 1 and rsi > RSI_OB:
            if last_closed['epoch'] <= bot_state["last_trade_epoch"]:
                log(f"⚠️ Ignored duplicate PUT signal for candle {candle_time} (Already fired).")
                return None
            log(f"🔥 PUT SIGNAL GENERATED! Breakout UP with RSI {rsi:.2f}")
            bot_state["last_signal"] = "PUT"
            bot_state["last_signal_time"] = bot_state["last_update"]
            bot_state["last_trade_epoch"] = last_closed['epoch']
            bot_state["pending_trade_details"] = {
                "rsi": rsi,
                "multiplier": multiplier_val,
                "body": last_closed['body_size'],
                "avg_body": last_closed['avg_body_size']
            }
            return "PUT"
        elif direction == -1 and rsi < RSI_OS:
            if last_closed['epoch'] <= bot_state["last_trade_epoch"]:
                log(f"⚠️ Ignored duplicate CALL signal for candle {candle_time} (Already fired).")
                return None
            log(f"🔥 CALL SIGNAL GENERATED! Breakout DOWN with RSI {rsi:.2f}")
            bot_state["last_signal"] = "CALL"
            bot_state["last_signal_time"] = bot_state["last_update"]
            bot_state["last_trade_epoch"] = last_closed['epoch']
            bot_state["pending_trade_details"] = {
                "rsi": rsi,
                "multiplier": multiplier_val,
                "body": last_closed['body_size'],
                "avg_body": last_closed['avg_body_size']
            }
            return "CALL"
            
    return None

async def run_bot_cycle():
    try:
        log("Running 15-minute strategy cycle...")
        candles = await fetch_recent_candles()
        if not candles:
            log("No candles returned. Skipping cycle.")
            return
            
        signal = check_for_signal(candles)
        
        if signal:
            await execute_trade(signal)
        else:
            log("No signal detected for this cycle.")
    except Exception as e:
        log(f"⚠️ Network Error during cycle: {e}. Bot will retry on the next cycle.")

async def handle(request):
    logs_html = "".join([f"<div class='log-entry'>{html.escape(l)}</div>" for l in app_logs])
    
    trades_html = ""
    for t in trade_history:
        profit = t.get("profit", 0)
        color = "var(--success)" if profit > 0 else ("var(--danger)" if profit < 0 else "var(--text-muted)")
        status = t.get("status", "OPEN")
        trades_html += f"""
        <div style="background: rgba(0,0,0,0.2); padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem; border-left: 4px solid {color};">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                <strong>{t.get('type')} - {t.get('time')}</strong>
                <span style="color: {color}; font-weight: bold;">{status} (P/L: ${profit:.2f})</span>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; font-size: 0.85rem; color: var(--text-muted);">
                <div>
                    <div>RSI: {t.get('details', {}).get('rsi', 0):.2f}</div>
                    <div>Multiplier: {t.get('details', {}).get('multiplier', 0):.2f}x</div>
                </div>
                <div>
                    <div>Bal Before: ${t.get('balance_before', 0):.2f}</div>
                    <div>Bal After: ${t.get('balance_after', 0):.2f}</div>
                </div>
            </div>
        </div>
        """
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Deriv Algo Bot Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=Fira+Code&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg: #0f172a;
                --surface: #1e293b;
                --primary: #3b82f6;
                --text: #f8fafc;
                --text-muted: #94a3b8;
                --success: #10b981;
                --danger: #ef4444;
            }}
            body {{
                margin: 0;
                font-family: 'Inter', system-ui, sans-serif;
                background-color: var(--bg);
                color: var(--text);
                padding: 2rem;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            .header {{
                display: flex;
                align-items: center;
                margin-bottom: 2rem;
            }}
            .header h1 {{
                margin: 0;
                font-size: 2rem;
                background: linear-gradient(to right, #60a5fa, #a78bfa);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }}
            .pulse {{
                width: 12px;
                height: 12px;
                background-color: var(--success);
                border-radius: 50%;
                margin-right: 1rem;
                box-shadow: 0 0 10px var(--success);
                animation: pulse 2s infinite;
            }}
            @keyframes pulse {{
                0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                70% {{ transform: scale(1); box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }}
                100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2rem;
            }}
            .card {{
                background: var(--surface);
                border-radius: 1rem;
                padding: 1.5rem;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                border: 1px solid rgba(255,255,255,0.05);
            }}
            .card h2 {{
                margin-top: 0;
                font-size: 1.1rem;
                color: var(--text-muted);
                text-transform: uppercase;
                letter-spacing: 0.05em;
            }}
            .stat {{
                font-size: 2.5rem;
                font-weight: 700;
                margin: 0.5rem 0;
            }}
            .sub-stat {{
                color: var(--text-muted);
                font-size: 0.9rem;
            }}
            .logs-container {{
                background: #000;
                border-radius: 0.5rem;
                padding: 1rem;
                height: 400px;
                overflow-y: auto;
                font-family: 'Fira Code', monospace;
                font-size: 0.85rem;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .log-entry {{
                margin-bottom: 0.35rem;
                color: #a3e635;
                padding-bottom: 0.35rem;
                border-bottom: 1px dashed rgba(255,255,255,0.1);
            }}
            .status-tag {{
                display: inline-block;
                padding: 0.25rem 0.75rem;
                border-radius: 9999px;
                font-size: 0.875rem;
                font-weight: 600;
                background-color: rgba(59, 130, 246, 0.1);
                color: #60a5fa;
            }}
            .val-up {{ color: var(--success); }}
            .val-down {{ color: var(--danger); }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="pulse"></div>
                <h1>Deriv Algo Bot Dashboard</h1>
            </div>
            
            <div class="grid">
                <div class="card">
                    <h2>Strategy Config</h2>
                    <div style="margin-top: 1rem; display: flex; flex-direction: column; gap: 0.5rem;">
                        <div style="display: flex; justify-content: space-between;">
                            <span class="sub-stat">Asset</span>
                            <strong>{SYMBOL} (15m)</strong>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span class="sub-stat">Stake</span>
                            <strong>${STAKE}</strong>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span class="sub-stat">Breakout Multiplier</span>
                            <strong>{MULTIPLIER}x - {MAX_MULTIPLIER}x</strong>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span class="sub-stat">RSI Settings</span>
                            <strong>{RSI_PERIOD} ({RSI_OB}/{RSI_OS})</strong>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <h2>Current Market State</h2>
                    <div class="sub-stat" style="margin-bottom: 1rem;">Last updated: {bot_state['last_update']}</div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                        <div>
                            <div class="sub-stat">RSI (7)</div>
                            <div style="font-size: 1.5rem; font-weight: bold;" class="{ 'val-up' if bot_state['last_candle_rsi'] > 50 else 'val-down'}">{bot_state['last_candle_rsi']:.2f}</div>
                        </div>
                        <div>
                            <div class="sub-stat">Body vs Avg</div>
                            <div style="font-size: 1.1rem; font-weight: bold;">{bot_state['last_candle_body']:.5f} / {bot_state['last_candle_avg_body']:.5f}</div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <h2>Account Overview</h2>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 1rem;">
                        <div>
                            <div class="sub-stat">Current Balance</div>
                            <div style="font-size: 1.5rem; font-weight: bold;">${bot_state['current_balance']:.2f}</div>
                            <div class="sub-stat" style="font-size: 0.75rem;">Start: ${bot_state['starting_balance']:.2f}</div>
                        </div>
                        <div>
                            <div class="sub-stat">Total P/L</div>
                            <div style="font-size: 1.5rem; font-weight: bold;" class="{ 'val-up' if bot_state['total_profit'] >= 0 else 'val-down'}">${bot_state['total_profit']:.2f}</div>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <h2>Bot Performance</h2>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 1rem;">
                        <div>
                            <div class="stat">{bot_state['total_trades']}</div>
                            <div class="sub-stat">Trades Executed</div>
                        </div>
                        <div>
                            <div style="font-size: 1.8rem; font-weight: 700; margin: 0.5rem 0;">
                                <span style="color: var(--success);">{bot_state['trades_won']}</span>
                                <span style="color: var(--text-muted); font-size: 1.2rem;">/</span>
                                <span style="color: var(--danger);">{bot_state['trades_lost']}</span>
                            </div>
                            <div class="sub-stat">Win / Loss</div>
                        </div>
                    </div>
                    
                    <div style="margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid rgba(255,255,255,0.05);">
                        <div class="sub-stat">Last Signal</div>
                        <div style="margin-top: 0.5rem;">
                            <span class="status-tag">{bot_state['last_signal']}</span>
                            <span class="sub-stat" style="margin-left: 0.5rem;">{bot_state['last_signal_time']}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card" style="margin-bottom: 2rem;">
                <h2>Trade History</h2>
                <div style="max-height: 400px; overflow-y: auto; padding-right: 0.5rem;">
                    {trades_html if trades_html else "<div class='sub-stat'>No trades yet.</div>"}
                </div>
            </div>

            <div class="card" style="padding: 0; overflow: hidden;">
                <h2 style="padding: 1.5rem 1.5rem 0.5rem 1.5rem;">System Logs</h2>
                <div class="logs-container">
                    {logs_html}
                </div>
            </div>
        </div>
        <script>
            setTimeout(() => window.location.reload(), 30000);
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type='text/html')

async def start_web_server():
    app = web.Application()
    app.add_routes([web.get('/', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8081))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    log(f"Web server started on port {port}")

async def bot_loop():
    while True:
        now = datetime.now()
        minutes = now.minute
        if minutes < 15: next_min = 15
        elif minutes < 30: next_min = 30
        elif minutes < 45: next_min = 45
        else: next_min = 60
        
        seconds_to_sleep = ((next_min * 60) - (minutes * 60 + now.second)) + 4
        
        if next_min == 60:
            log(f"Sleeping for {seconds_to_sleep} seconds until next hour...")
        else:
            log(f"Sleeping for {seconds_to_sleep} seconds until XX:{next_min}:04...")
            
        await asyncio.sleep(seconds_to_sleep)
        await run_bot_cycle()

async def main():
    load_dotenv()
    if not os.environ.get("DERIV_API_TOKEN") or not os.environ.get("DERIV_ACCOUNT_ID") or not os.environ.get("DERIV_APP_ID"):
        log("ERROR: Missing API credentials in .env file.")
        return
        
    bal = await fetch_balance()
    if bal is not None:
        bot_state["starting_balance"] = bal
        bot_state["current_balance"] = bal
        log(f"Fetched starting balance: ${bal:.2f}")
    else:
        log("Warning: Could not fetch initial balance.")

    await start_web_server()
    log("Live Forex Bot Started! Waiting for the next 15-minute boundary...")
    
    log("Running initial test cycle immediately...")
    await run_bot_cycle()
    await bot_loop()

if __name__ == "__main__":
    asyncio.run(main())
