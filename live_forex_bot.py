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
SYMBOL = "frxAUDCAD"
STAKE = 10.0
ROLLING_WINDOW = 20
MULTIPLIER = 2.5
RSI_PERIOD = 7
RSI_OB = 80
RSI_OS = 20
DURATION = 15
DURATION_UNIT = "m"

# --- GLOBAL STATE FOR DASHBOARD ---
bot_state = {
    "total_trades": 0,
    "last_signal": "None",
    "last_signal_time": "Never",
    "last_candle_rsi": 0.0,
    "last_candle_body": 0.0,
    "last_candle_avg_body": 0.0,
    "last_update": "Never"
}
app_logs = deque(maxlen=100)

def log(msg):
    time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted = f"[{time_str}] {msg}"
    print(formatted)
    app_logs.appendleft(formatted)

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
            "basis": "payout",
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
            
        proposal_id = res.get("proposal", {}).get("id")
        ask_price = res.get("proposal", {}).get("ask_price")
        log(f"Obtained Quote -> Price: {ask_price}, Proposal ID: {proposal_id}")
        
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
            log(f"Trade Executed Successfully! Contract ID: {buy_res['buy']['contract_id']}")
            bot_state["total_trades"] += 1

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
    df = df.iloc[:-1].copy()
    
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    
    df['returns'] = df['close'] - df['open']
    df['body_size'] = abs(df['returns'])
    df['direction'] = np.where(df['returns'] > 0, 1, -1)
    
    df['avg_body_size'] = df['body_size'].rolling(window=ROLLING_WINDOW).mean().shift(1)
    df['rsi'] = calculate_rsi(df['close'].values, RSI_PERIOD)
    
    last_closed = df.iloc[-1]
    
    if pd.isna(last_closed['avg_body_size']) or pd.isna(last_closed['rsi']):
        return None
        
    is_breakout = last_closed['body_size'] > (last_closed['avg_body_size'] * MULTIPLIER)
    rsi = last_closed['rsi']
    direction = last_closed['direction']
    
    # Update state for dashboard
    bot_state["last_candle_rsi"] = float(rsi)
    bot_state["last_candle_body"] = float(last_closed['body_size'])
    bot_state["last_candle_avg_body"] = float(last_closed['avg_body_size'])
    bot_state["last_update"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    candle_time = pd.to_datetime(last_closed['epoch'], unit='s')
    log(f"Analyzed 15m candle (Epoch: {candle_time}) -> Body: {last_closed['body_size']:.5f}, Avg: {last_closed['avg_body_size']:.5f}, RSI: {rsi:.2f}")

    if is_breakout:
        if direction == 1 and rsi > RSI_OB:
            log(f"🔥 PUT SIGNAL GENERATED! Breakout UP with RSI {rsi:.2f}")
            bot_state["last_signal"] = "PUT"
            bot_state["last_signal_time"] = bot_state["last_update"]
            return "PUT"
        elif direction == -1 and rsi < RSI_OS:
            log(f"🔥 CALL SIGNAL GENERATED! Breakout DOWN with RSI {rsi:.2f}")
            bot_state["last_signal"] = "CALL"
            bot_state["last_signal_time"] = bot_state["last_update"]
            return "CALL"
            
    return None

async def run_bot_cycle():
    log("Running 15-minute strategy cycle...")
    candles = await fetch_recent_candles()
    signal = check_for_signal(candles)
    
    if signal:
        await execute_trade(signal)
    else:
        log("No signal detected for this cycle.")

async def handle(request):
    logs_html = "".join([f"<div class='log-entry'>{html.escape(l)}</div>" for l in app_logs])
    
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
                            <strong>{MULTIPLIER}x</strong>
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
                    <h2>Bot Performance</h2>
                    <div class="stat">{bot_state['total_trades']}</div>
                    <div class="sub-stat">Trades Executed</div>
                    
                    <div style="margin-top: 1.5rem;">
                        <div class="sub-stat">Last Signal</div>
                        <div style="margin-top: 0.5rem;">
                            <span class="status-tag">{bot_state['last_signal']}</span>
                            <span class="sub-stat" style="margin-left: 0.5rem;">{bot_state['last_signal_time']}</span>
                        </div>
                    </div>
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
    port = int(os.environ.get("PORT", 8080))
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
        
        seconds_to_sleep = ((next_min * 60) - (minutes * 60 + now.second)) + 1
        
        if next_min == 60:
            log(f"Sleeping for {seconds_to_sleep} seconds until next hour...")
        else:
            log(f"Sleeping for {seconds_to_sleep} seconds until XX:{next_min}:01...")
            
        await asyncio.sleep(seconds_to_sleep)
        await run_bot_cycle()

async def main():
    load_dotenv()
    if not os.environ.get("DERIV_API_TOKEN") or not os.environ.get("DERIV_ACCOUNT_ID") or not os.environ.get("DERIV_APP_ID"):
        log("ERROR: Missing API credentials in .env file.")
        return
        
    await start_web_server()
    log("Live Forex Bot Started! Waiting for the next 15-minute boundary...")
    
    log("Running initial test cycle immediately...")
    await run_bot_cycle()
    await bot_loop()

if __name__ == "__main__":
    asyncio.run(main())
