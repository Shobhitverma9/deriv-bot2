import asyncio
import websockets
import json
import pandas as pd
import numpy as np
import requests
import os
import collections
from aiohttp import web
from datetime import datetime

# --- IN-MEMORY STATE ---
log_history = collections.deque(maxlen=200)

BOT_STATE = {
    "start_time": datetime.utcnow(),
    "status": "Initializing...",
    "disconnects": 0,
    "trades_placed": 0,
    "last_trade_epoch": 0
}

def get_uptime():
    diff = datetime.utcnow() - BOT_STATE["start_time"]
    hours, remainder = divmod(int(diff.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"

def custom_print(*args, **kwargs):
    """Overrides print to also store output in the in-memory log deque."""
    import builtins
    message = " ".join(map(str, args))
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    formatted_msg = f"[{timestamp}] {message}"
    log_history.append(formatted_msg)
    builtins.print(formatted_msg, **kwargs)

print = custom_print

# --- STRATEGY CONFIG ---
SYMBOL = "R_100"        # Changed to Volatility 100 Index to fix the Crypto duration limit
GRANULARITY = 300       # 5 minutes
ROLLING_WINDOW = 20
BREAKOUT_MULTIPLIER = 2.5
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
STAKE_AMOUNT = 10.00    # Flat stake for initial live testing

def load_env():
    env_vars = {}
    try:
        with open('.env', 'r') as f:
            for line in f:
                line = line.split('#')[0].strip()
                if line:
                    key, val = line.split('=', 1)
                    env_vars[key.strip()] = val.strip()
    except FileNotFoundError:
        pass 
    return env_vars

def calculate_rsi(prices, periods=14):
    if len(prices) < periods + 1:
        return 50
    delta = np.diff(prices)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    avg_gain = np.mean(gain[-periods:])
    avg_loss = np.mean(loss[-periods:])
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def execute_trade(api_token, app_id, account_id, contract_type):
    print(f"\n🚀 EXECUTING {contract_type} TRADE...")
    url = 'https://api.derivws.com/trading/v1/options/contracts/bulk-purchase/demo'
    
    headers = {
        'Deriv-App-ID': app_id,
        'Content-Type': 'application/json'
    }
    
    payload = {
        "contract_parameters": {
            "amount": STAKE_AMOUNT,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "underlying_symbol": SYMBOL,
            "duration": 5,
            "duration_unit": "m"
        },
        "accounts": [
            {
                "account_id": account_id,
                "token": api_token
            }
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        data = response.json()
        
        if 'errors' in data and len(data['errors']) > 0:
            print(f"❌ TRADE REJECTED by Broker:")
            print(json.dumps(data['errors'], indent=2))
        else:
            BOT_STATE['trades_placed'] += 1
            print(f"✅ TRADE PLACED SUCCESSFULLY!")
            print(json.dumps(data.get('data', {}), indent=2))
            
    except Exception as e:
        print(f"❌ HTTP ERROR while placing trade: {e}")

async def process_strategy(env, candles):
    if len(candles) < max(ROLLING_WINDOW, RSI_PERIOD) + 1:
        return
        
    df = pd.DataFrame(candles[:-1])
    df['returns'] = df['close'] - df['open']
    df['body_size'] = abs(df['returns'])
    df['direction'] = np.where(df['returns'] > 0, 1, -1)
    df['avg_body_size'] = df['body_size'].rolling(window=ROLLING_WINDOW).mean()
    
    last_row = df.iloc[-1]
    prev_avg = df.iloc[-2]['avg_body_size'] if len(df) > 1 else last_row['avg_body_size']
    
    is_breakout = last_row['body_size'] > (prev_avg * BREAKOUT_MULTIPLIER)
    breakout_dir = last_row['direction']
    rsi = calculate_rsi(df['close'].values, RSI_PERIOD)
    
    if is_breakout:
        print(f"\n🔥 MASSIVE BREAKOUT DETECTED! Direction: {'UP' if breakout_dir==1 else 'DOWN'}")
        print(f"Body Size: {last_row['body_size']:.2f}, Average: {prev_avg:.2f}, RSI: {rsi:.2f}")
        
        contract_type = None
        if breakout_dir == 1 and rsi > RSI_OVERBOUGHT:
            print("🚨 RSI Exhaustion (Overbought). Signal: FALL contract (PUT).")
            contract_type = "PUT"
        elif breakout_dir == -1 and rsi < RSI_OVERSOLD:
            print("🚨 RSI Exhaustion (Oversold). Signal: RISE contract (CALL).")
            contract_type = "CALL"
        else:
            print("⚠️ RSI not at extremes. Ignoring breakout to avoid trend-continuation trap.")
            
        if contract_type:
            current_epoch = last_row['epoch'] if 'epoch' in last_row else int(datetime.utcnow().timestamp())
            if current_epoch <= BOT_STATE['last_trade_epoch']:
                print(f"⚠️ Ignored duplicate signal for candle {current_epoch} (Already fired).")
            else:
                BOT_STATE['last_trade_epoch'] = current_epoch
                api_token = env.get('DERIV_API_TOKEN')
                app_id = env.get('DERIV_APP_ID')
                account_id = env.get('DERIV_ACCOUNT_ID')
                await asyncio.to_thread(execute_trade, api_token, app_id, account_id, contract_type)

async def live_trading_bot():
    env = load_env()
    api_token = os.environ.get('DERIV_API_TOKEN', env.get('DERIV_API_TOKEN', ''))
    app_id = os.environ.get('DERIV_APP_ID', env.get('DERIV_APP_ID', ''))
    account_id = os.environ.get('DERIV_ACCOUNT_ID', env.get('DERIV_ACCOUNT_ID', ''))
    
    env['DERIV_API_TOKEN'] = api_token
    env['DERIV_APP_ID'] = app_id
    env['DERIV_ACCOUNT_ID'] = account_id

    if not api_token or not app_id or not account_id:
        print("ERROR: Missing credentials.")
        BOT_STATE["status"] = "ERROR: Missing credentials."
        return

    uri = "wss://api.derivws.com/trading/v1/options/ws/public"
    
    while True:
        try:
            candles = []
            BOT_STATE["status"] = "Connecting..."
            print(f"Connecting to Deriv Public Data Stream...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                BOT_STATE["status"] = f"Connected. Subscribing to {SYMBOL}..."
                print(f"Connected. Subscribing to {SYMBOL} 5-minute candles...")
                
                sub_request = {
                    "ticks_history": SYMBOL,
                    "adjust_start_time": 1,
                    "count": max(ROLLING_WINDOW, RSI_PERIOD) + 5, 
                    "end": "latest",
                    "style": "candles",
                    "granularity": GRANULARITY,
                    "subscribe": 1
                }
                await ws.send(json.dumps(sub_request))
                
                async for message in ws:
                    data = json.loads(message)
                    
                    if 'error' in data:
                        print(f"API Error: {data['error']['message']}")
                        continue
                        
                    msg_type = data.get('msg_type')
                    
                    if msg_type == 'candles':
                        history = data['candles']
                        for c in history[:-1]:
                            candles.append({
                                'epoch': c['epoch'],
                                'open': float(c['open']),
                                'high': float(c['high']),
                                'low': float(c['low']),
                                'close': float(c['close'])
                            })
                        msg = f"Seeded {len(candles)} historical candles. Bot is fully armed and scanning."
                        print(msg)
                        BOT_STATE["status"] = "Active and scanning..."
                        
                    elif msg_type == 'ohlc':
                        ohlc = data['ohlc']
                        epoch = int(ohlc['epoch'])
                        
                        if len(candles) > 0 and epoch > candles[-1]['epoch']:
                            new_candle = {
                                'epoch': epoch,
                                'open': float(ohlc['open']),
                                'high': float(ohlc['high']),
                                'low': float(ohlc['low']),
                                'close': float(ohlc['close'])
                            }
                            
                            if candles[-1]['epoch'] != epoch:
                                candles.append(new_candle)
                                if len(candles) > 100:
                                    candles.pop(0) 
                                    
                                await process_strategy(env, candles)
                        
                        elif len(candles) > 0 and epoch == candles[-1]['epoch']:
                            candles[-1]['high'] = max(candles[-1]['high'], float(ohlc['high']))
                            candles[-1]['low'] = min(candles[-1]['low'], float(ohlc['low']))
                            candles[-1]['close'] = float(ohlc['close'])
                            
        except websockets.ConnectionClosed as e:
            BOT_STATE["disconnects"] += 1
            BOT_STATE["status"] = "Disconnected. Reconnecting..."
            print(f"⚠️ WebSocket disconnected ({e}). Reconnecting in 5 seconds...")
        except Exception as e:
            BOT_STATE["disconnects"] += 1
            BOT_STATE["status"] = "Error. Reconnecting..."
            print(f"❌ Unexpected error: {e}. Reconnecting in 5 seconds...")
        
        await asyncio.sleep(5)

# --- WEB DASHBOARD SERVER ---

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Deriv Algo-Bot Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0f172a;
            --panel: rgba(30, 41, 59, 0.7);
            --accent: #38bdf8;
            --text: #f8fafc;
            --success: #10b981;
            --danger: #ef4444;
            --border: rgba(255,255,255,0.1);
        }
        body {
            background-color: var(--bg);
            color: var(--text);
            font-family: 'Inter', sans-serif;
            margin: 0;
            padding: 2rem;
            background-image: radial-gradient(circle at top right, #1e1b4b, #0f172a);
            min-height: 100vh;
        }
        h1 { font-weight: 800; font-size: 2.5rem; margin-bottom: 2rem; letter-spacing: -1px; text-align: center; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }
        .card { background: var(--panel); backdrop-filter: blur(12px); border: 1px solid var(--border); border-radius: 16px; padding: 1.5rem; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -1px rgba(0,0,0,0.06); transition: transform 0.2s; }
        .card:hover { transform: translateY(-2px); border-color: rgba(255,255,255,0.2); }
        .card h3 { margin: 0 0 1rem 0; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; color: #94a3b8; }
        .value { font-size: 2rem; font-weight: 600; margin: 0; }
        .status-dot { display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: var(--success); margin-right: 8px; box-shadow: 0 0 10px var(--success); }
        .status-dot.offline { background: var(--danger); box-shadow: 0 0 10px var(--danger); }
        .logs-container { background: #020617; border: 1px solid var(--border); border-radius: 12px; padding: 1rem; height: 400px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #cbd5e1; box-shadow: inset 0 2px 4px 0 rgba(0,0,0,0.5); }
        .log-line { margin: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 4px; line-height: 1.4; }
        .log-line.error { color: #f87171; }
        .log-line.trade { color: #34d399; font-weight: bold; }
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #64748b; }
    </style>
</head>
<body>
    <h1>Deriv Algo-Bot</h1>
    
    <div class="grid">
        <div class="card">
            <h3>System Status</h3>
            <div class="value" style="font-size: 1.2rem; display: flex; align-items: center; height: 100%;">
                <span id="status-dot" class="status-dot"></span>
                <span id="status-text">Loading...</span>
            </div>
            <p style="margin: 10px 0 0 0; font-size: 0.85rem; color: #94a3b8;">Uptime: <span id="uptime">0s</span></p>
        </div>
        
        <div class="card">
            <h3>Trading Performance</h3>
            <p class="value" id="trades-placed" style="color: var(--accent);">0</p>
            <p style="margin: 10px 0 0 0; font-size: 0.85rem; color: #94a3b8;">Trades Executed</p>
        </div>
        
        <div class="card">
            <h3>Network Stability</h3>
            <p class="value" id="disconnects" style="color: #f472b6;">0</p>
            <p style="margin: 10px 0 0 0; font-size: 0.85rem; color: #94a3b8;">Disconnects Handled</p>
        </div>
        
        <div class="card">
            <h3>Current Parameters</h3>
            <div style="font-size: 0.9rem; color: #cbd5e1; line-height: 1.6;">
                <div>Symbol: <strong style="color:white;" id="param-symbol">-</strong></div>
                <div>Stake: <strong style="color:white;" id="param-stake">-</strong></div>
                <div>Breakout Mult: <strong style="color:white;" id="param-mult">-</strong></div>
                <div>RSI Thresholds: <strong style="color:white;" id="param-rsi">-</strong></div>
            </div>
        </div>
    </div>

    <div class="card" style="margin-bottom: 0;">
        <h3 style="display: flex; justify-content: space-between;">
            Live Terminal
            <span style="font-size: 0.75rem; background: var(--bg); padding: 4px 8px; border-radius: 4px; border: 1px solid var(--border);">Live updating</span>
        </h3>
        <div class="logs-container" id="logs">
            <!-- Logs injected here -->
        </div>
    </div>

    <script>
        function formatLogLine(line) {
            let className = 'log-line';
            if (line.includes('❌') || line.includes('Error') || line.includes('disconnected') || line.includes('⚠️')) {
                className += ' error';
            } else if (line.includes('🚀') || line.includes('✅') || line.includes('🔥')) {
                className += ' trade';
            }
            return `<div class="${className}">${line}</div>`;
        }

        async function fetchData() {
            try {
                const res = await fetch('/api/data');
                const data = await res.json();
                
                // Update text
                document.getElementById('status-text').innerText = data.status;
                document.getElementById('uptime').innerText = data.uptime;
                document.getElementById('trades-placed').innerText = data.trades_placed;
                document.getElementById('disconnects').innerText = data.disconnects;
                
                document.getElementById('param-symbol').innerText = data.symbol;
                document.getElementById('param-stake').innerText = '$' + data.stake.toFixed(2);
                document.getElementById('param-mult').innerText = data.multiplier + 'x';
                document.getElementById('param-rsi').innerText = `${data.rsi_oversold} / ${data.rsi_overbought}`;
                
                // Update dot
                const dot = document.getElementById('status-dot');
                if (data.status.includes('Connected') || data.status.includes('scanning')) {
                    dot.className = 'status-dot';
                } else {
                    dot.className = 'status-dot offline';
                }

                // Update logs
                const logsDiv = document.getElementById('logs');
                const isScrolledToBottom = logsDiv.scrollHeight - logsDiv.clientHeight <= logsDiv.scrollTop + 10;
                
                logsDiv.innerHTML = data.logs.map(formatLogLine).join('');
                
                if (isScrolledToBottom) {
                    logsDiv.scrollTop = logsDiv.scrollHeight;
                }
                
            } catch (e) {
                document.getElementById('status-text').innerText = 'Dashboard Disconnected';
                document.getElementById('status-dot').className = 'status-dot offline';
            }
        }

        setInterval(fetchData, 1500); 
        fetchData(); 
    </script>
</body>
</html>
"""

async def ping_handler(request):
    """Keep-alive ping endpoint"""
    return web.Response(text="OK", status=200)

async def dashboard_handler(request):
    """Serves the HTML dashboard UI"""
    return web.Response(text=DASHBOARD_HTML, content_type='text/html')

async def api_data_handler(request):
    """Serves the live data to the dashboard frontend"""
    data = {
        "status": BOT_STATE["status"],
        "uptime": get_uptime(),
        "disconnects": BOT_STATE["disconnects"],
        "trades_placed": BOT_STATE["trades_placed"],
        "symbol": SYMBOL,
        "stake": STAKE_AMOUNT,
        "multiplier": BREAKOUT_MULTIPLIER,
        "rsi_oversold": RSI_OVERSOLD,
        "rsi_overbought": RSI_OVERBOUGHT,
        "logs": list(log_history)
    }
    return web.json_response(data)

async def init_web_server():
    app = web.Application()
    app.add_routes([
        web.get('/ping', ping_handler),
        web.get('/api/data', api_data_handler),
        web.get('/', dashboard_handler),
        web.get('/logs', dashboard_handler) # Reroute /logs to the dashboard
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    print(f"Web server started on port {port}. Available endpoints: /, /ping")
    await site.start()

async def main():
    print("========================================")
    print(" DERIV ALGO-BOT: RSI EXHAUSTION SYSTEM  ")
    print("========================================")
    
    await asyncio.gather(
        init_web_server(),
        live_trading_bot()
    )

if __name__ == "__main__":
    asyncio.run(main())
