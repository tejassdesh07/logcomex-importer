#!/usr/bin/env python3
from pyngrok import ngrok
import time
import signal
import sys

def signal_handler(sig, frame):
    print('\nShutting down ngrok tunnel...')
    ngrok.kill()
    sys.exit(0)

# Set up signal handler for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)

# Set auth token and connect
ngrok.set_auth_token('2yXhWfSaMEKAiP2hN695qHRMtna_2qNey6RCJJLRnq8NiAhQ6')
tunnel = ngrok.connect(8000)

print(f"🚀 Ngrok tunnel is active!")
print(f"📡 Public URL: {tunnel.public_url}")
print(f"🔗 Local URL: http://localhost:8000")
print(f"📊 Ngrok dashboard: http://localhost:4040")
print(f"\n✅ Your FastAPI app is now publicly accessible at: {tunnel.public_url}")
print("\n🛑 Press Ctrl+C to stop the tunnel")

try:
    # Keep the script running
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('\nShutting down ngrok tunnel...')
    ngrok.kill()
