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
SYMBOL = "frxGBPUSD"
# --- STAKING CONFIGURATION ---
MIN_STAKE = 5.0
BASE_RISK_PCT = 0.03
ROLLING_WINDOW = 15
MULTIPLIER = 3.5
MAX_MULTIPLIER = 99.0
RSI_PERIOD = 7
RSI_OB = 80
RSI_OS = 20
DURATION = 15
DURATION_UNIT = "m"

# --- RISK & FILTER SETTINGS ---
MIN_PAYOUT_PCT = 70.0
USE_TIME_FILTER = True
BLACKOUT_START_HOUR = 6  # 06:00 GMT (London)
BLACKOUT_END_HOUR = 8    # 08:00 GMT
NY_BLACKOUT_START_HOUR = 12 # 12:00 GMT (New York)
NY_BLACKOUT_END_HOUR = 15   # 15:00 GMT (Covers 12, 13, 14)
BLOCK_THURSDAYS = True   # Disable trading on Thursdays
MIN_SMA_DISTANCE = 0.015 # Don't trade if too close to SMA

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
    try:
        save_state()
    except Exception as e:
        print(f"Error saving state: {e}")

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
        bal = await fetch_balance()
        if bal is not None:
            bot_state["current_balance"] = bal
        else:
            bal = bot_state["current_balance"]
            
        if bal < MIN_STAKE:
            log(f"⚠️ Insufficient balance to meet minimum stake of ${MIN_STAKE}.")
            return
            
        raw_stake = bal * BASE_RISK_PCT
        dynamic_stake = max(MIN_STAKE, min(raw_stake, bal * 0.25))
        dynamic_stake = round(dynamic_stake, 2)
        log(f"Dynamic Stake Calculated: {BASE_RISK_PCT*100}% of ${bal:.2f} = ${dynamic_stake}")

        proposal_req = {
            "proposal": 1,
            "amount": dynamic_stake,
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
            "count": 100, 
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
    
    df['sma_50'] = df['close'].rolling(window=50).mean()
    df['dist_to_sma'] = abs((df['close'] - df['sma_50']) / df['close']) * 100
    
    last_closed = df.iloc[-1]
    
    # Time Filter Check
    candle_time = pd.to_datetime(last_closed['epoch'], unit='s')
    current_hour = candle_time.hour
    
    if USE_TIME_FILTER:
        if (BLACKOUT_START_HOUR <= current_hour < BLACKOUT_END_HOUR) or (NY_BLACKOUT_START_HOUR <= current_hour < NY_BLACKOUT_END_HOUR):
            bot_state["last_update"] = f"{candle_time} (Blackout Period)"
            log(f"⏸️ Signal ignored. {current_hour}:00 GMT is within a blackout window.")
            return None
        
    if BLOCK_THURSDAYS and candle_time.dayofweek == 3:
        bot_state["last_update"] = f"{candle_time} (Thursday Block)"
        log("⏸️ Signal ignored. Trading is disabled on Thursdays to avoid macro volatility traps.")
        return None
        
    if pd.isna(last_closed['avg_body_size']) or pd.isna(last_closed['rsi']) or pd.isna(last_closed['sma_50']):
        return None
        
    if last_closed['dist_to_sma'] < MIN_SMA_DISTANCE:
        bot_state["last_update"] = f"{candle_time} (SMA Reject)"
        log(f"⏸️ Signal ignored. Breakout too close to moving average ({last_closed['dist_to_sma']:.3f}% < {MIN_SMA_DISTANCE}%). No rubber-band effect.")
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


def save_state():
    os.makedirs("logs", exist_ok=True)
    state_file = f"logs/state_{SYMBOL}.json"
    
    # Merge all useful state into one object
    full_state = {
        "symbol": SYMBOL,
        "bot_state": bot_state,
        "app_logs": list(app_logs),
        "trade_history": list(trade_history)
    }
    
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(full_state, f, indent=4)


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

    log("Live Forex Bot Started! Waiting for the next 15-minute boundary...")
    
    log("Running initial test cycle immediately...")
    await run_bot_cycle()
    await bot_loop()

if __name__ == "__main__":
    asyncio.run(main())
