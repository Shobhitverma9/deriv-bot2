import subprocess
import sys
import time

bots = [
    "live_eurgbp_bot.py",
    "live_usdjpy_bot.py",
    "live_usdchf_bot.py",
    "live_gbpusd_bot.py",
    "live_eurusd_bot.py",
    "live_audusd_bot.py",
    "live_audcad_bot.py",
    "dashboard.py"
]

processes = []

print("🚀 Launching the Magnificent Seven Trading Fleet...")

for bot in bots:
    print(f"Starting {bot}...")
    # Launch each bot as a separate subprocess
    p = subprocess.Popen([sys.executable, bot])
    processes.append((bot, p))
    # Slight delay to prevent API rate limiting on startup
    time.sleep(2)

print("\n✅ All 7 bots are now running in the background!")
print("Press Ctrl+C to stop the entire fleet.\n")

try:
    # Keep the main script alive and monitor the bots
    while True:
        for bot, p in processes:
            if p.poll() is not None:
                print(f"⚠️ Warning: {bot} has stopped with exit code {p.returncode}")
        time.sleep(10)
except KeyboardInterrupt:
    print("\n🛑 Stopping the fleet...")
    for bot, p in processes:
        p.terminate()
    print("Fleet gracefully shut down.")
