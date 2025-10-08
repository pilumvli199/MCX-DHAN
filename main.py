"""
MCX Commodities Market Data Tracker - FIXED VERSION
Enhanced debugging and error handling
"""

import time
import requests
from datetime import datetime
import threading
import os
import json

# ==================== CONFIGURATION ====================
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

DHAN_API_BASE = "https://api.dhan.co/v2"

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

UPDATE_INTERVAL = 300

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
    """Fetch complete market data for all MCX commodities"""
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
        
        print(f"🔄 Sending request to DhanHQ API...")
        print(f"   Endpoint: {url}")
        print(f"   Commodities: {len(MCX_COMMODITIES)}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"📡 Response Status: {response.status_code}")
        
        # DEBUG: Print full response
        print(f"\n🔍 DEBUG - Full API Response:")
        print(json.dumps(response.json(), indent=2))
        
        if response.status_code == 200:
            data = response.json()
            
            # Try different possible response structures
            mcx_data = None
            
            # Structure 1: data.MCX_COM
            if 'data' in data and 'MCX_COM' in data['data']:
                mcx_data = data['data']['MCX_COM']
                print("✅ Found data in: data.MCX_COM")
            
            # Structure 2: MCX_COM directly
            elif 'MCX_COM' in data:
                mcx_data = data['MCX_COM']
                print("✅ Found data in: MCX_COM")
            
            # Structure 3: data as direct dict
            elif 'data' in data and isinstance(data['data'], dict):
                mcx_data = data['data']
                print("✅ Found data in: data (direct dict)")
            
            # Structure 4: Direct list or dict
            elif isinstance(data, dict):
                # Check if any keys are security IDs
                for key in data.keys():
                    if key.isdigit() or isinstance(key, int):
                        mcx_data = data
                        print("✅ Found data in: root (security IDs as keys)")
                        break
            
            if mcx_data:
                print(f"✅ Processing {len(mcx_data)} items from response")
                
                for security_id, commodity_name in MCX_COMMODITIES.items():
                    # Try both string and int keys
                    security_data = mcx_data.get(str(security_id)) or mcx_data.get(security_id)
                    
                    if security_data:
                        # Extract data with multiple field name possibilities
                        ltp = float(security_data.get('last_price', 
                                   security_data.get('ltp', 
                                   security_data.get('lastPrice', 0))))
                        
                        # OHLC Data
                        ohlc = security_data.get('ohlc', {})
                        open_price = float(ohlc.get('open', 0))
                        high_price = float(ohlc.get('high', 0))
                        low_price = float(ohlc.get('low', 0))
                        prev_close = float(ohlc.get('close', 0))
                        
                        # Volume & OI Data
                        volume = int(security_data.get('volume', 
                                    security_data.get('traded_volume', 0)))
                        oi = int(security_data.get('oi', 
                                security_data.get('open_interest', 0)))
                        
                        # Other Data
                        buy_qty = int(security_data.get('buy_quantity', 
                                     security_data.get('buyQty', 0)))
                        sell_qty = int(security_data.get('sell_quantity', 
                                      security_data.get('sellQty', 0)))
                        
                        if ltp > 0:
                            change_pct = calculate_change(ltp, prev_close)
                            previous_prices[security_id] = ltp
                            
                            if change_pct > 0:
                                emoji = "🟢"
                                change_text = f"+{change_pct:.2f}%"
                            elif change_pct < 0:
                                emoji = "🔴"
                                change_text = f"{change_pct:.2f}%"
                            else:
                                emoji = "⚪"
                                change_text = "0.00%"
                            
                            commodity_msg = [
                                f"\n{emoji} <b>{commodity_name}</b>",
                                f"├ LTP: {format_price(ltp)} ({change_text})",
                            ]
                            
                            if open_price > 0 or high_price > 0 or low_price > 0:
                                commodity_msg.append(
                                    f"├ O: {format_price(open_price)} | H: {format_price(high_price)} | L: {format_price(low_price)}"
                                )
                            
                            if volume > 0 or oi > 0:
                                vol_oi_line = "├"
                                if volume > 0:
                                    vol_oi_line += f" Vol: {format_number(volume)}"
                                if oi > 0:
                                    if volume > 0:
                                        vol_oi_line += " |"
                                    vol_oi_line += f" OI: {format_number(oi)}"
                                commodity_msg.append(vol_oi_line)
                            
                            if buy_qty > 0 or sell_qty > 0:
                                commodity_msg.append(f"└ Buy: {format_number(buy_qty)} | Sell: {format_number(sell_qty)}")
                            else:
                                commodity_msg.append("└─────────────────")
                            
                            message_lines.extend(commodity_msg)
                            
                            print(f"\n{emoji} {commodity_name}")
                            print(f"  LTP: {format_price(ltp):12s} | Change: {change_text:8s}")
                            if open_price > 0:
                                print(f"  O: {format_price(open_price):10s} H: {format_price(high_price):10s} L: {format_price(low_price):10s}")
                            
                            success_count += 1
                        else:
                            print(f"⚪ {commodity_name:15s}: No LTP data (LTP={ltp})")
                    else:
                        print(f"⚠️ {commodity_name:15s}: Not found in response")
            else:
                print("⚠️ Could not find MCX data in any expected structure")
                print(f"Response structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        
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
        
    except Exception as e:
        error_msg = f"❌ Unexpected error: {str(e)}"
        print(error_msg)
        print(f"Full error: {repr(e)}")
        import traceback
        traceback.print_exc()
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
    print("🚀 MCX COMMODITIES MARKET DATA TRACKER - FIXED VERSION")
    print("="*70)
    print(f"📊 Tracking: {len(MCX_COMMODITIES)} commodities")
    print(f"⏱️  Update: Every {UPDATE_INTERVAL} seconds")
    print(f"📈 Enhanced debugging enabled")
    print("="*70 + "\n")
    
    if DHAN_CLIENT_ID == "YOUR_DHAN_CLIENT_ID":
        print("❌ ERROR: Set DHAN_CLIENT_ID!")
        return
    
    if DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN":
        print("❌ ERROR: Set DHAN_ACCESS_TOKEN!")
        return
    
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print("⚠️ WARNING: Telegram not configured")
    
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
