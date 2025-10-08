"""
MCX Commodities Market Data Tracker - WORKING VERSION
Uses Direct REST API calls (No SDK dependency issues!)
Fetches complete market data of MCX commodities
Sends formatted alerts to Telegram

Requirements:
pip install requests
"""

import time
import requests
from datetime import datetime
import threading
import os

# ==================== CONFIGURATION ====================
# DhanHQ API Credentials
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# DhanHQ API Base URL
DHAN_API_BASE = "https://api.dhan.co/v2"

# MCX Commodity Security IDs
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

# Update interval in seconds (300 = 5 min, 60 = 1 min)
UPDATE_INTERVAL = 300  # 5 minutes

# ==================== GLOBAL VARIABLES ====================
previous_prices = {}
bot_running = True

# ==================== FUNCTIONS ====================

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

def fetch_market_data():
    """Fetch complete market data for all MCX commodities using REST API"""
    global previous_prices
    
    print(f"\n{'='*70}")
    print(f"⏰ Fetching Market Data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    message_lines = [
        "📊 <b>MCX COMMODITIES - MARKET DATA</b>",
        f"🕐 {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]
    
    success_count = 0
    
    try:
        # Prepare request for Market Quote API
        # Endpoint: POST /marketfeed/quote
        url = f"{DHAN_API_BASE}/marketfeed/quote"
        
        headers = {
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Request payload: {"MCX_COM": [security_ids]}
        payload = {
            "MCX_COM": list(MCX_COMMODITIES.keys())
        }
        
        print(f"🔄 Sending request to DhanHQ API...")
        print(f"   Endpoint: {url}")
        print(f"   Commodities: {len(MCX_COMMODITIES)}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and 'MCX_COM' in data['data']:
                mcx_data = data['data']['MCX_COM']
                
                for security_id, commodity_name in MCX_COMMODITIES.items():
                    security_data = mcx_data.get(str(security_id))
                    
                    if security_data:
                        # Extract all available data
                        ltp = float(security_data.get('last_price', 0))
                        
                        # OHLC Data
                        ohlc = security_data.get('ohlc', {})
                        open_price = float(ohlc.get('open', 0))
                        high_price = float(ohlc.get('high', 0))
                        low_price = float(ohlc.get('low', 0))
                        prev_close = float(ohlc.get('close', 0))
                        
                        # Volume & OI Data
                        volume = int(security_data.get('volume', 0))
                        oi = int(security_data.get('oi', 0))
                        
                        # Other Data
                        buy_qty = int(security_data.get('buy_quantity', 0))
                        sell_qty = int(security_data.get('sell_quantity', 0))
                        net_change = float(security_data.get('net_change', 0))
                        
                        if ltp > 0:
                            # Calculate change percentage
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
                            
                            # Build detailed message
                            commodity_msg = [
                                f"\n{emoji} <b>{commodity_name}</b>",
                                f"├ LTP: {format_price(ltp)} ({change_text})",
                            ]
                            
                            # Add OHLC if available
                            if open_price > 0 or high_price > 0 or low_price > 0:
                                commodity_msg.append(
                                    f"├ O: {format_price(open_price)} | H: {format_price(high_price)} | L: {format_price(low_price)}"
                                )
                            
                            # Add Volume and OI if available
                            if volume > 0 or oi > 0:
                                vol_oi_line = "├"
                                if volume > 0:
                                    vol_oi_line += f" Vol: {format_number(volume)}"
                                if oi > 0:
                                    if volume > 0:
                                        vol_oi_line += " |"
                                    vol_oi_line += f" OI: {format_number(oi)}"
                                commodity_msg.append(vol_oi_line)
                            
                            # Add Buy/Sell quantities
                            if buy_qty > 0 or sell_qty > 0:
                                commodity_msg.append(f"└ Buy: {format_number(buy_qty)} | Sell: {format_number(sell_qty)}")
                            else:
                                commodity_msg.append("└─────────────────")
                            
                            message_lines.extend(commodity_msg)
                            
                            # Console output
                            print(f"\n{emoji} {commodity_name}")
                            print(f"  LTP: {format_price(ltp):12s} | Change: {change_text:8s}")
                            if open_price > 0:
                                print(f"  O: {format_price(open_price):10s} H: {format_price(high_price):10s} L: {format_price(low_price):10s}")
                            if volume > 0 or oi > 0:
                                print(f"  Vol: {format_number(volume):15s} | OI: {format_number(oi)}")
                            
                            success_count += 1
                        else:
                            print(f"⚪ {commodity_name:15s}: No LTP data")
                    else:
                        print(f"⚠️ {commodity_name:15s}: Not found in response")
            else:
                print("⚠️ No MCX_COM data in response")
                print(f"Response keys: {data.get('data', {}).keys() if 'data' in data else 'No data key'}")
        
        elif response.status_code == 429:
            error_msg = "⚠️ Rate limit exceeded. Waiting longer..."
            print(error_msg)
            send_telegram_message(error_msg)
            
        else:
            error_msg = f"❌ API Error {response.status_code}: {response.text}"
            print(error_msg)
            send_telegram_message(f"❌ API Error: {response.status_code}")
        
        # Send to Telegram
        if success_count > 0:
            message_lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            message_lines.append(f"✅ {success_count}/{len(MCX_COMMODITIES)} commodities")
            message_lines.append(f"⏱️ Next update in {UPDATE_INTERVAL//60} min")
            
            message = "\n".join(message_lines)
            
            if send_telegram_message(message):
                print(f"\n✅ Alert sent to Telegram!")
            else:
                print(f"\n⚠️ Telegram alert failed")
        else:
            error_msg = "❌ No data fetched for any commodity"
            print(error_msg)
            send_telegram_message(error_msg)
                
    except requests.exceptions.Timeout:
        error_msg = "⏱️ API request timeout. Will retry..."
        print(error_msg)
        send_telegram_message(error_msg)
        
    except requests.exceptions.RequestException as e:
        error_msg = f"❌ Network error: {str(e)}"
        print(error_msg)
        send_telegram_message(error_msg)
        
    except Exception as e:
        error_msg = f"❌ Unexpected error: {str(e)}"
        print(error_msg)
        print(f"Full error: {repr(e)}")
        send_telegram_message(error_msg)

def send_startup_message():
    """Send bot startup notification"""
    interval_text = f"{UPDATE_INTERVAL//60} minute(s)" if UPDATE_INTERVAL >= 60 else f"{UPDATE_INTERVAL} second(s)"
    
    message = (
        "🤖 <b>MCX Market Data Tracker Started!</b>\n\n"
        f"🕐 {datetime.now().strftime('%d-%m-%Y %I:%M:%S %p')}\n"
        f"📊 Tracking: {len(MCX_COMMODITIES)} commodities\n"
        f"⏱️ Updates: Every {interval_text}\n\n"
        "📈 Data includes:\n"
        "• LTP & Price Change %\n"
        "• OHLC (Open/High/Low/Close)\n"
        "• Volume & Open Interest\n"
        "• Buy/Sell Quantities\n\n"
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
    print(f"📊 Tracking: {len(MCX_COMMODITIES)} commodities")
    print(f"⏱️  Update: Every {UPDATE_INTERVAL} seconds")
    print(f"📈 Data: LTP, OHLC, Volume, OI, Buy/Sell")
    print(f"🔌 Using: Direct REST API (No SDK issues!)")
    print("="*70 + "\n")
    
    # Validate credentials
    if DHAN_CLIENT_ID == "YOUR_DHAN_CLIENT_ID":
        print("❌ ERROR: Set DHAN_CLIENT_ID!")
        print("Get from: https://web.dhan.co → Profile → Access DhanHQ APIs")
        return
    
    if DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN":
        print("❌ ERROR: Set DHAN_ACCESS_TOKEN!")
        return
    
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print("⚠️ WARNING: Telegram not configured")
    
    # Test DhanHQ connection
    print("🔄 Testing DhanHQ connection...\n")
    if not validate_credentials():
        print("\n❌ Cannot connect to DhanHQ!")
        print("Please check:")
        print("  1. Your access token is valid")
        print("  2. Data API subscription (₹499/month) is active")
        print("  3. Internet connection is working")
        return
    
    # Send startup notification
    print("\n")
    send_startup_message()
    
    print("\n✅ Bot is running! Press Ctrl+C to stop.\n")
    
    # Start scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Keep main thread alive
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
