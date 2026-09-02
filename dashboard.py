import os
import json
import glob
import asyncio
from aiohttp import web
from datetime import datetime

async def handle(request):
    # Read all state files
    state_files = glob.glob("logs/state_*.json")
    
    fleet_states = []
    global_profit = 0.0
    global_wins = 0
    global_losses = 0
    global_balance = 0.0
    
    for f in state_files:
        try:
            with open(f, "r", encoding="utf-8") as file:
                data = json.load(file)
                fleet_states.append(data)
                
                bs = data.get("bot_state", {})
                global_profit += bs.get("total_profit", 0.0)
                global_wins += bs.get("trades_won", 0)
                global_losses += bs.get("trades_lost", 0)
                # Just take the balance from one of them (they all share the same account)
                # But since they update at different times, we'll take the max or just the first non-zero
                if bs.get("current_balance", 0.0) > global_balance:
                    global_balance = bs.get("current_balance", 0.0)
        except Exception:
            pass

    # Sort fleet by profit
    fleet_states.sort(key=lambda x: x.get("bot_state", {}).get("total_profit", 0.0), reverse=True)

    # Build Bot Cards HTML
    cards_html = ""
    for data in fleet_states:
        symbol = data.get("symbol", "UNKNOWN")
        bs = data.get("bot_state", {})
        
        profit = bs.get('total_profit', 0)
        profit_color = "var(--success)" if profit >= 0 else "var(--danger)"
        
        rsi = bs.get('last_candle_rsi', 0)
        rsi_color = "var(--success)" if rsi > 50 else "var(--danger)"
        
        wins = bs.get('trades_won', 0)
        losses = bs.get('trades_lost', 0)
        
        cards_html += f"""
        <div class="card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h2>{symbol}</h2>
                <div class="status-tag">Active</div>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                <div>
                    <div class="sub-stat">Total P/L</div>
                    <div style="font-size: 1.5rem; font-weight: bold; color: {profit_color};">${profit:.2f}</div>
                </div>
                <div>
                    <div class="sub-stat">Win / Loss</div>
                    <div style="font-size: 1.2rem; font-weight: bold;">
                        <span style="color: var(--success);">{wins}</span> / 
                        <span style="color: var(--danger);">{losses}</span>
                    </div>
                </div>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; padding-top: 1rem; border-top: 1px solid rgba(255,255,255,0.05);">
                <div>
                    <div class="sub-stat">Live RSI (7)</div>
                    <div style="font-size: 1.2rem; font-weight: bold; color: {rsi_color};">{rsi:.2f}</div>
                </div>
                <div>
                    <div class="sub-stat">Last Signal</div>
                    <div style="font-size: 0.9rem; margin-top: 0.2rem;">{bs.get('last_signal', 'None')}</div>
                    <div style="font-size: 0.75rem; color: var(--text-muted);">{bs.get('last_signal_time', 'Never')}</div>
                </div>
            </div>
        </div>
        """

    # Build HTML
    total_trades = global_wins + global_losses
    win_rate = (global_wins / total_trades * 100) if total_trades > 0 else 0.0
    global_profit_color = "var(--success)" if global_profit >= 0 else "var(--danger)"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Hedge Fund Command Center</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg: #09090b;
                --surface: rgba(24, 24, 27, 0.7);
                --primary: #6366f1;
                --text: #f8fafc;
                --text-muted: #a1a1aa;
                --success: #10b981;
                --danger: #ef4444;
            }}
            body {{
                margin: 0;
                font-family: 'Outfit', sans-serif;
                background-color: var(--bg);
                color: var(--text);
                background-image: 
                    radial-gradient(at 0% 0%, hsla(253,16%,7%,1) 0, transparent 50%), 
                    radial-gradient(at 50% 0%, hsla(225,39%,30%,0.1) 0, transparent 50%), 
                    radial-gradient(at 100% 0%, hsla(339,49%,30%,0.1) 0, transparent 50%);
                background-attachment: fixed;
                min-height: 100vh;
                padding: 2rem;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            .header {{
                display: flex;
                align-items: center;
                margin-bottom: 3rem;
            }}
            .header h1 {{
                margin: 0;
                font-size: 2.5rem;
                font-weight: 800;
                background: linear-gradient(to right, #818cf8, #c084fc);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                letter-spacing: -0.05em;
            }}
            .pulse {{
                width: 14px;
                height: 14px;
                background-color: var(--success);
                border-radius: 50%;
                margin-right: 1rem;
                box-shadow: 0 0 15px var(--success);
                animation: pulse 2s infinite;
            }}
            @keyframes pulse {{
                0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                70% {{ transform: scale(1); box-shadow: 0 0 0 15px rgba(16, 185, 129, 0); }}
                100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
            }}
            
            /* Glassmorphism Overview Panel */
            .overview-panel {{
                background: var(--surface);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 1.5rem;
                padding: 2.5rem;
                margin-bottom: 3rem;
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 2rem;
                box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
            }}
            .overview-stat h3 {{
                margin: 0 0 0.5rem 0;
                color: var(--text-muted);
                font-size: 1rem;
                text-transform: uppercase;
                letter-spacing: 0.1em;
            }}
            .overview-stat .value {{
                font-size: 3.5rem;
                font-weight: 800;
                letter-spacing: -0.02em;
            }}
            
            /* Grid for Bots */
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
                gap: 1.5rem;
            }}
            .card {{
                background: var(--surface);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border-radius: 1rem;
                padding: 1.5rem;
                border: 1px solid rgba(255,255,255,0.05);
                transition: transform 0.2s ease, border-color 0.2s ease;
            }}
            .card:hover {{
                transform: translateY(-5px);
                border-color: rgba(255,255,255,0.15);
            }}
            .card h2 {{
                margin: 0;
                font-size: 1.5rem;
                font-weight: 600;
                letter-spacing: -0.02em;
            }}
            .sub-stat {{
                color: var(--text-muted);
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 0.05em;
                margin-bottom: 0.25rem;
            }}
            .status-tag {{
                padding: 0.25rem 0.75rem;
                border-radius: 9999px;
                font-size: 0.75rem;
                font-weight: 600;
                background-color: rgba(16, 185, 129, 0.15);
                color: var(--success);
                text-transform: uppercase;
                letter-spacing: 0.1em;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="pulse"></div>
                <h1>Fleet Command Center</h1>
            </div>
            
            <div class="overview-panel">
                <div class="overview-stat">
                    <h3>Total Net Profit</h3>
                    <div class="value" style="color: {global_profit_color};">${global_profit:.2f}</div>
                </div>
                <div class="overview-stat">
                    <h3>Global Win Rate</h3>
                    <div class="value">{win_rate:.1f}%</div>
                </div>
                <div class="overview-stat">
                    <h3>Total Trades</h3>
                    <div class="value">{total_trades}</div>
                </div>
                <div class="overview-stat">
                    <h3>Account Balance</h3>
                    <div class="value">${global_balance:.2f}</div>
                </div>
            </div>
            
            <h2 style="font-weight: 600; letter-spacing: -0.02em; margin-bottom: 1.5rem; color: var(--text-muted);">Active Algorithms</h2>
            <div class="grid">
                {cards_html if cards_html else "<div style='color: var(--text-muted);'>No active bots detected. Waiting for state files...</div>"}
            </div>
        </div>
        
        <script>
            // Auto-refresh every 5 seconds to keep dashboard live
            setTimeout(() => window.location.reload(), 5000);
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type='text/html')

async def main():
    app = web.Application()
    app.add_routes([web.get('/', handle)])
    
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    print(f"Master Dashboard running on port {port}...")
    await site.start()
    
    # Keep server alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
