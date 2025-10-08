"""
MCX Commodities Market Data Tracker - MARKET HOURS FIX
Checks market status and handles empty responses
"""

import time
import requests
from datetime import datetime, time as dt_time
import threading
import os
import json

# ==================== CONFIGURATION ====================
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

DHAN_API_BASE = "https://api.dhan.co/v2"

# Using CURRENT MONTH contracts (October 2025)
# These are the most liquid near-month contracts
MCX_COMMODITIES = {
    # Major commodities with high liquidity
    466583: "GOLD",      # Gold standard lot
    477904: "GOLDM",     # Gold mini
    471725: "SILVER",    # Silver standard
    471726: "SILVERM",   # Silver mini
    472789: "CRUDEOIL",  # Crude Oil
    475111: "NATURALGAS",# Natural Gas
    477167: "COPPER",    # Copper
    477171: "ZINC",      # Zinc
    477166: "ALUMINIUM", # Aluminium
}

UPDATE_INTERVAL = 300

previous_prices = {}
bot_running = True

# ==================== FUNCTIONS ====================

def is_market_hours():
    """Check if MCX market is open"""
    now = datetime.now()
    current_time = now.time()
    
    # MCX timing: Monday-Friday, 9:00 AM to 11:30 PM (with break 5:00-5:30 PM)
    # Saturday half day: 9:00 AM to 2:00 PM
    
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    if weekday == 6:  # Sunday
        return False
    
    morning_start = dt_time(9, 0)
    evening_end = dt_time(23, 30)
    
    if weekday == 5:  # Saturday
        saturday_end = dt_time(14, 0)
        return morning_start <= current_time <= saturday_end
    else:  # Monday to Friday
        return morning_start <= current_time <= evening_end

def validate_credentials():
    """Validate DhanHQ credentials"""
    try:
        url = f"{DHAN_API_BASE}/profile"
        headers = {
            "access-token": DHAN_ACCESS_TOKEN,
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Connected to DhanHQ!")
            print(f"   Client ID: {data.get('dhanClientId', 'N/A')}")
            print(f"   Active Segments: {data.get('activeSegment', 'N/A')}")
            print(f"   Data Plan: {data.get('dataPlan', 'N/A')}")
            print(f"   Data Validity: {data.get('dataValidity', 'N/A')}")
            
            # Check if MCX segment is active
            segments = data.get('activeSegment', '')
            if 'M' in segments or 'MCX' in segments:
                print(f"   ✅ MCX Segment: Active")
            else:
                print(f"   ⚠️ MCX Segment: Not found in active segments")
            
            return True
        else:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Connection Error: {e}")
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
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Telegram Error: {e}")
        return False

def format_price(price):
    """Format price with proper decimal places"""
    if price:
        return f"₹{price:,.2f}"
    return "N/A"

def format_number(num):
    """Format large numbers with commas"""
    if num:
        return f"{num:,}"
    return "0"

def calculate_change(current, previous):
    """Calculate price change percentage"""
    if previous and current and previous != 0:
        change = ((current - previous) / previous) * 100
        return change
    return 0

def try_alternative_endpoints(security_id, commodity_name):
    """Try alternative API endpoints for single commodity"""
    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json"
    }
    
    # Try LTP endpoint (simpler, just last traded price)
    try:
        url = f"{DHAN_API_BASE}/marketfeed/ltp"
        payload = {"MCX_COM": [security_id]}
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"   📊 LTP API Response for {commodity_name}:")
            print(f"      {json.dumps(data, indent=6)}")
            return data
    except Exception as e:
        print(f"   ⚠️ LTP API failed for {commodity_name}: {e}")
    
    return None

def fetch_market_data():
    """Fetch complete market data for all MCX commodities"""
    global previous_prices
    
    print(f"\n{'='*70}")
    print(f"⏰ Fetching Market Data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check market hours
    if not is_market_hours():
        print(f"⏰ MCX Market is CLOSED")
        print(f"   Trading Hours: Mon-Fri 9:00 AM - 11:30 PM")
        print(f"                  Saturday 9:00 AM - 2:00 PM")
        send_telegram_message(
            "⏰ <b>MCX Market is CLOSED</b>\n\n"
            f"🕐 Current Time: {datetime.now().strftime('%I:%M:%S %p')}\n"
            f"📅 {datetime.now().strftime('%A, %d %B %Y')}\n\n"
            "Trading Hours:\n"
            "• Mon-Fri: 9:00 AM - 11:30 PM\n"
            "• Saturday: 9:00 AM - 2:00 PM\n"
            "• Sunday: Closed"
        )
        print(f"{'='*70}")
        return
    
    print(f"✅ MCX Market is OPEN")
    print(f"{'='*70}")
    
    message_lines = [
        "📊 <b>MCX COMMODITIES - MARKET DATA</b>",
        f"🕐 {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]
    
    success_count = 0
    
    try:
        # Method 1: Batch quote API
        url = f"{DHAN_API_BASE}/marketfeed/quote"
        
        headers = {
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        payload = {
            "MCX_COM": list(MCX_COMMODITIES.keys())
        }
        
        print(f"🔄 Trying Batch Quote API...")
        print(f"   Endpoint: {url}")
        print(f"   Security IDs: {list(MCX_COMMODITIES.keys())}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"📡 Response Status: {response.status_code}")
        print(f"\n🔍 Full API Response:")
        response_data = response.json()
        print(json.dumps(response_data, indent=2))
        
        # Check if data is empty
        if response.status_code == 200:
            data = response_data
            
            if 'data' in data and (not data['data'] or data['data'] == {}):
                print(f"\n⚠️ Empty data response - trying alternative methods...")
                
                # Try individual commodity requests
                print(f"\n🔄 Trying individual commodity requests...")
                for security_id, commodity_name in MCX_COMMODITIES.items():
                    alt_data = try_alternative_endpoints(security_id, commodity_name)
                    time.sleep(0.5)  # Rate limiting
        
        # If still no success, send diagnostic message
        if success_count == 0:
            diag_msg = (
                "⚠️ <b>No MCX Data Available</b>\n\n"
                "Possible reasons:\n"
                "1. Market is in pre-open/closed session\n"
                "2. Security IDs are expired contracts\n"
                "3. Need current month contracts\n"
                "4. MCX Data Pack subscription issue\n\n"
                f"API Status: {response.status_code}\n"
                f"Response: {data.get('status', 'Unknown')}\n\n"
                "Check DhanHQ web platform to verify:\n"
                "• MCX commodities are trading\n"
                "• Your data pack includes MCX\n"
                "• Security IDs are current month contracts"
            )
            print(diag_msg.replace('<b>', '').replace('</b>', ''))
            send_telegram_message(diag_msg)
        
    except requests.exceptions.Timeout:
        error_msg = "⏱️ API request timeout. Will retry..."
        print(error_msg)
        send_telegram_message(error_msg)
        
    except Exception as e:
        error_msg = f"❌ Unexpected error: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        send_telegram_message(error_msg)

def send_startup_message():
    """Send bot startup notification"""
    interval_text = f"{UPDATE_INTERVAL//60} minute(s)" if UPDATE_INTERVAL >= 60 else f"{UPDATE_INTERVAL} second(s)"
    
    market_status = "🟢 OPEN" if is_market_hours() else "🔴 CLOSED"
    
    message = (
        "🤖 <b>MCX Market Data Tracker Started!</b>\n\n"
        f"🕐 {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}\n"
        f"📊 Tracking: {len(MCX_COMMODITIES)} commodities\n"
        f"⏱️ Updates: Every {interval_text}\n"
        f"🏪 Market Status: {market_status}\n\n"
        "📈 Commodities:\n"
        f"{', '.join(MCX_COMMODITIES.values())}\n\n"
        "✅ Bot is now running..."
    )
    
    if send_telegram_message(message):
        print(message.replace('<b>', '').replace('</b>', ''))
    else:
        print("⚠️ Could not send startup message")

def run_scheduler():
    """Run the scheduler loop"""
    global bot_running
    
    while bot_running:
        try:
            fetch_market_data()
            print(f"\n💤 Waiting {UPDATE_INTERVAL} seconds for next update...")
            time.sleep(UPDATE_INTERVAL)
        except Exception as e:
            print(f"❌ Scheduler error: {e}")
            time.sleep(10)

def main():
    """Main function"""
    global bot_running
    
    print("\n" + "="*70)
    print("🚀 MCX COMMODITIES MARKET DATA TRACKER")
    print("="*70)
    print(f"📊 Tracking: {len(MCX_COMMODITIES)} major commodities")
    print(f"⏱️  Update: Every {UPDATE_INTERVAL} seconds")
    print(f"🕐 Market Hours Check: Enabled")
    print("="*70 + "\n")
    
    if DHAN_CLIENT_ID == "YOUR_DHAN_CLIENT_ID":
        print("❌ ERROR: Set DHAN_CLIENT_ID!")
        return
    
    if DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN":
        print("❌ ERROR: Set DHAN_ACCESS_TOKEN!")
        return
    
    print("🔄 Testing DhanHQ connection...\n")
    if not validate_credentials():
        print("\n❌ Cannot connect to DhanHQ!")
        return
    
    print("\n")
    send_startup_message()
    
    print("\n✅ Bot is running! Press Ctrl+C to stop.\n")
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Bot stopped by user")
        bot_running = False
        send_telegram_message("🛑 <b>MCX Tracker Stopped</b>")
        print("👋 Goodbye!")

if __name__ == "__main__":
    main()
