"""
MCX Commodities LTP Tracker Bot
Fetches Last Traded Price (LTP) of MCX commodities every 1 minute
Sends alerts to Telegram

Requirements:
pip install dhanhq requests schedule
"""

import time
import schedule
import requests
from dhanhq import dhanhq
from datetime import datetime

# ==================== CONFIGURATION ====================
# DhanHQ API Credentials
DHAN_CLIENT_ID = "YOUR_DHAN_CLIENT_ID"  # Tumcha DhanHQ Client ID
DHAN_ACCESS_TOKEN = "YOUR_DHAN_ACCESS_TOKEN"  # Tumcha Access Token

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"  # BotFather pasun milel
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"  # Tumcha Chat ID

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

# ==================== GLOBAL VARIABLES ====================
dhan = None
previous_prices = {}

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
            print(f"⚠️ Telegram API Error: {response.status_code}")
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
    
    security_ids = list(MCX_COMMODITIES.keys())
    
    try:
        # Fetch LTP data from DhanHQ
        response = dhan.get_ltp_data(
            exchange_segment=dhan.MCX,
            security_id=security_ids
        )
        
        if response and 'data' in response:
            ltp_data = response['data']
            
            for security_id, commodity_name in MCX_COMMODITIES.items():
                # Find matching data
                commodity_data = next(
                    (item for item in ltp_data if item.get('security_id') == security_id),
                    None
                )
                
                if commodity_data:
                    ltp = commodity_data.get('LTP', 0)
                    prev_close = commodity_data.get('prev_close', 0)
                    
                    # Calculate change
                    prev_ltp = previous_prices.get(security_id, ltp)
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
                else:
                    message_lines.append(f"⚠️ <b>{commodity_name}</b>: Data not available")
                    print(f"⚠️ {commodity_name:15s}: Data not available")
            
            # Send to Telegram
            message = "\n".join(message_lines)
            if send_telegram_message(message):
                print(f"\n✅ Alert sent to Telegram successfully!")
            else:
                print(f"\n⚠️ Failed to send Telegram alert")
                
        else:
            error_msg = "❌ No data received from DhanHQ API"
            print(error_msg)
            send_telegram_message(error_msg)
            
    except Exception as e:
        error_msg = f"❌ Error fetching LTP data: {str(e)}"
        print(error_msg)
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
    send_telegram_message(message)
    print(message.replace('<b>', '').replace('</b>', ''))

def main():
    """Main function"""
    print("\n" + "="*60)
    print("🚀 MCX COMMODITIES LTP TRACKER BOT")
    print("="*60)
    print(f"📊 Tracking {len(MCX_COMMODITIES)} commodities")
    print(f"⏱️  Update interval: Every 1 minute")
    print("="*60 + "\n")
    
    # Initialize DhanHQ
    if not initialize_dhan():
        print("❌ Cannot start bot without DhanHQ connection!")
        return
    
    # Send startup notification
    send_startup_message()
    
    # Fetch initial data immediately
    print("📥 Fetching initial data...")
    fetch_ltp_data()
    
    # Schedule to run every 1 minute
    schedule.every(1).minutes.do(fetch_ltp_data)
    
    print("\n✅ Bot is running! Press Ctrl+C to stop.\n")
    
    # Keep the bot running
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Bot stopped by user")
        send_telegram_message("🛑 <b>MCX LTP Tracker Bot Stopped</b>")

if __name__ == "__main__":
    main()
