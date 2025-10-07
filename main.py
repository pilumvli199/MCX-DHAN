"""
MCX Commodities LTP Tracker Bot
Fetches Last Traded Price (LTP) of MCX commodities every 1 minute
Sends alerts to Telegram

Requirements:
pip install dhanhq requests
"""

import time
import requests
from dhanhq import dhanhq
from datetime import datetime
import threading
import os

# ==================== CONFIGURATION ====================
# DhanHQ API Credentials (Environment variables la priority)
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")

# Telegram Bot Configuration (Environment variables la priority)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# MCX Commodity Security IDs (Latest Expiry)
MCX_COMMODITIES = {
    477166: "ALUMINIUM",
    477183: "CARDAMOM",
    477167: "COPPER",
    467748: "COTTON",
    477182: "COTTONOIL",
    472789: "CRUDEOIL",
    472790: "CRUDEOILM",
    466583: "GOLD",
    477174: "GOLDGUINEA",
    477904: "GOLDM",
    477175: "GOLDPETAL",
    477176: "GOLDTEN",
    477168: "LEAD",
    477169: "LEADMINI",
    477184: "MENTHAOIL",
    475111: "NATURALGAS",
    477173: "NICKEL",
    471725: "SILVER",
    471726: "SILVERM",
    477177: "SILVERMIC",
    477171: "ZINC",
    477172: "ZINCMINI",
}

# Update interval in seconds (60 seconds = 1 minute)
UPDATE_INTERVAL = 60

# ==================== GLOBAL VARIABLES ====================
dhan = None
previous_prices = {}
bot_running = True

# ==================== FUNCTIONS ====================

def initialize_dhan():
    """Initialize DhanHQ connection"""
    global dhan
    try:
        dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        print("✅ DhanHQ connection initialized successfully!")
        return True
    except Exception as e:
        print(f"❌ Error initializing DhanHQ: {e}")
        return False

def send_telegram_message(message):
    """Send message to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            return True
        else:
            print(f"⚠️ Telegram API Error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error sending Telegram message: {e}")
        return False

def format_price(price):
    """Format price with proper decimal places"""
    if price:
        return f"₹{price:,.2f}"
    return "N/A"

def calculate_change(current, previous):
    """Calculate price change percentage"""
    if previous and current and previous != 0:
        change = ((current - previous) / previous) * 100
        return change
    return 0

def fetch_ltp_data():
    """Fetch LTP for all MCX commodities"""
    global previous_prices
    
    print(f"\n{'='*60}")
    print(f"⏰ Fetching LTP at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    message_lines = [
        "📊 <b>MCX COMMODITIES LTP UPDATE</b>",
        f"🕐 Time: {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]
    
    success_count = 0
    
    try:
        # Prepare securities dictionary for batch fetch
        # Format: {"MCX": [security_id1, security_id2, ...]}
        securities = {
            "MCX": list(MCX_COMMODITIES.keys())
        }
        
        # Fetch LTP data using ticker_data (for LTP) or ohlc_data (for OHLC + LTP)
        response = dhan.ticker_data(securities=securities)
        
        if response and 'data' in response and 'MCX' in response['data']:
            mcx_data = response['data']['MCX']
            
            for security_id, commodity_name in MCX_COMMODITIES.items():
                # Find data for this security_id
                security_data = mcx_data.get(str(security_id))
                
                if security_data:
                    ltp = float(security_data.get('LTP', 0))
                    prev_close = float(security_data.get('prev_close', 0))
                    
                    if ltp > 0:
                        # Calculate change
                        change_pct = calculate_change(ltp, prev_close)
                        
                        # Update previous prices
                        previous_prices[security_id] = ltp
                        
                        # Emoji based on change
                        if change_pct > 0:
                            emoji = "🟢"
                            change_text = f"+{change_pct:.2f}%"
                        elif change_pct < 0:
                            emoji = "🔴"
                            change_text = f"{change_pct:.2f}%"
                        else:
                            emoji = "⚪"
                            change_text = "0.00%"
                        
                        # Format message
                        line = f"{emoji} <b>{commodity_name}</b>: {format_price(ltp)} ({change_text})"
                        message_lines.append(line)
                        
                        # Console output
                        print(f"{emoji} {commodity_name:15s}: {format_price(ltp):12s} | Change: {change_text:8s}")
                        success_count += 1
                    else:
                        print(f"⚪ {commodity_name:15s}: No LTP data")
                else:
                    print(f"⚠️ {commodity_name:15s}: Not found in response")
        else:
            print("⚠️ No MCX data in response")
            print(f"Response: {response}")
        
        # Send to Telegram if we got at least some data
        if success_count > 0:
            message_lines.append(f"\n✅ Successfully fetched {success_count}/{len(MCX_COMMODITIES)} commodities")
            message = "\n".join(message_lines)
            
            if send_telegram_message(message):
                print(f"\n✅ Alert sent to Telegram successfully!")
            else:
                print(f"\n⚠️ Failed to send Telegram alert")
        else:
            error_msg = "❌ Failed to fetch data for all commodities"
            print(error_msg)
            send_telegram_message(error_msg)
                
    except Exception as e:
        error_msg = f"❌ Error fetching LTP data: {str(e)}"
        print(error_msg)
        print(f"Full error details: {repr(e)}")
        send_telegram_message(error_msg)

def send_startup_message():
    """Send bot startup notification"""
    message = (
        "🤖 <b>MCX LTP Tracker Bot Started!</b>\n\n"
        f"🕐 Started at: {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}\n"
        f"📊 Tracking {len(MCX_COMMODITIES)} commodities\n"
        f"⏱️ Update Interval: Every 1 minute\n\n"
        "✅ Bot is now running..."
    )
    if send_telegram_message(message):
        print(message.replace('<b>', '').replace('</b>', ''))
    else:
        print("⚠️ Could not send startup message - check Telegram credentials")

def run_scheduler():
    """Run the scheduler loop"""
    global bot_running
    
    while bot_running:
        try:
            fetch_ltp_data()
            # Sleep for UPDATE_INTERVAL seconds
            print(f"\n💤 Waiting {UPDATE_INTERVAL} seconds for next update...")
            time.sleep(UPDATE_INTERVAL)
        except Exception as e:
            print(f"❌ Error in scheduler: {e}")
            time.sleep(10)  # Wait 10 seconds before retrying

def main():
    """Main function"""
    global bot_running
    
    print("\n" + "="*60)
    print("🚀 MCX COMMODITIES LTP TRACKER BOT")
    print("="*60)
    print(f"📊 Tracking {len(MCX_COMMODITIES)} commodities")
    print(f"⏱️  Update interval: Every {UPDATE_INTERVAL} seconds")
    print("="*60 + "\n")
    
    # Check if credentials are set
    if DHAN_CLIENT_ID == "YOUR_DHAN_CLIENT_ID":
        print("❌ ERROR: Please set your DHAN_CLIENT_ID in the code!")
        return
    
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print("⚠️ WARNING: Telegram bot token not set - alerts will not work!")
    
    # Initialize DhanHQ
    if not initialize_dhan():
        print("❌ Cannot start bot without DhanHQ connection!")
        return
    
    # Send startup notification
    send_startup_message()
    
    print("\n✅ Bot is running! Press Ctrl+C to stop.\n")
    
    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Bot stopped by user")
        bot_running = False
        send_telegram_message("🛑 <b>MCX LTP Tracker Bot Stopped</b>")
        print("👋 Goodbye!")

if __name__ == "__main__":
    main()
