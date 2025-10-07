"""
Helper Script to Fetch MCX Commodity Security IDs from DhanHQ
This script downloads the scrip master file and extracts MCX commodity IDs
"""

import requests
import csv
from io import StringIO
from datetime import datetime

# URL for DhanHQ scrip master
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Popular MCX commodities to search for
POPULAR_COMMODITIES = [
    "GOLD",
    "SILVER", 
    "CRUDEOIL",
    "NATURALGAS",
    "COPPER",
    "ZINC",
    "LEAD",
    "ALUMINIUM",
    "NICKEL",
    "MENTHAOIL",
    "CARDAMOM",
    "COTTON",
]

def download_scrip_master():
    """
    Download the scrip master CSV file from DhanHQ
    """
    print("📥 Downloading scrip master file from DhanHQ...")
    try:
        response = requests.get(SCRIP_MASTER_URL, timeout=30)
        response.raise_for_status()
        print("✓ Downloaded successfully\n")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading file: {e}")
        return None

def parse_mcx_commodities(csv_content):
    """
    Parse CSV and extract MCX commodity information
    """
    print("🔍 Parsing MCX commodities...\n")
    
    csv_reader = csv.DictReader(StringIO(csv_content))
    mcx_commodities = []
    
    for row in csv_reader:
        # Check if it's MCX exchange and Commodity segment
        if row.get('SEM_EXM_EXCH_ID') == 'MCX' and row.get('SEM_SEGMENT') == 'M':
            security_id = row.get('SEM_SMST_SECURITY_ID')
            symbol = row.get('SEM_TRADING_SYMBOL', '')
            expiry = row.get('SEM_EXPIRY_DATE', '')
            
            # Check if it's a popular commodity
            commodity_base = symbol.split('-')[0] if '-' in symbol else symbol
            
            if any(comm in commodity_base.upper() for comm in POPULAR_COMMODITIES):
                mcx_commodities.append({
                    'security_id': security_id,
                    'symbol': symbol,
                    'expiry': expiry,
                    'base_name': commodity_base
                })
    
    return mcx_commodities

def display_commodities(commodities):
    """
    Display found commodities in a formatted way
    """
    if not commodities:
        print("❌ No MCX commodities found!")
        return
    
    print("=" * 80)
    print("📊 MCX COMMODITIES FOUND")
    print("=" * 80)
    print(f"{'Security ID':<15} {'Symbol':<30} {'Expiry Date':<15}")
    print("-" * 80)
    
    # Group by base commodity name
    grouped = {}
    for comm in commodities:
        base = comm['base_name']
        if base not in grouped:
            grouped[base] = []
        grouped[base].append(comm)
    
    for base_name in sorted(grouped.keys()):
        print(f"\n🔸 {base_name.upper()}")
        for comm in grouped[base_name][:3]:  # Show only first 3 expiries
            print(f"  {comm['security_id']:<15} {comm['symbol']:<30} {comm['expiry']:<15}")
    
    print("\n" + "=" * 80)

def generate_code_snippet(commodities):
    """
    Generate code snippet to copy-paste into main bot
    """
    print("\n" + "=" * 80)
    print("📋 CODE SNIPPET FOR YOUR BOT")
    print("=" * 80)
    print("\n# Copy and paste this into your bot.py file:\n")
    
    # Get latest expiry for each commodity
    latest_commodities = {}
    for comm in commodities:
        base = comm['base_name']
        if base not in latest_commodities:
            latest_commodities[base] = comm
        else:
            # Compare expiry dates (assuming format DD-MMM-YYYY)
            if comm['expiry'] > latest_commodities[base]['expiry']:
                latest_commodities[base] = comm
    
    print("MCX_COMMODITIES = {")
    print('    "MCX_COMM": [')
    
    for base_name in sorted(latest_commodities.keys()):
        comm = latest_commodities[base_name]
        print(f"        {comm['security_id']},  # {comm['symbol']}")
    
    print("    ]")
    print("}")
    
    print("\n# Optional: Commodity name mapping")
    print("COMMODITY_NAMES = {")
    for base_name in sorted(latest_commodities.keys()):
        comm = latest_commodities[base_name]
        display_name = base_name.upper()
        print(f"    {comm['security_id']}: \"{display_name}\",")
    print("}")
    
    print("\n" + "=" * 80)

def save_to_file(commodities):
    """
    Save commodity details to a text file
    """
    filename = f"mcx_commodities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("MCX COMMODITIES - DHANHQ SECURITY IDs\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Group by base commodity
            grouped = {}
            for comm in commodities:
                base = comm['base_name']
                if base not in grouped:
                    grouped[base] = []
                grouped[base].append(comm)
            
            for base_name in sorted(grouped.keys()):
                f.write(f"\n{base_name.upper()}\n")
                f.write("-" * 80 + "\n")
                for comm in grouped[base_name]:
                    f.write(f"ID: {comm['security_id']:<12} | Symbol: {comm['symbol']:<25} | Expiry: {comm['expiry']}\n")
        
        print(f"\n💾 Details saved to: {filename}")
        return True
    except Exception as e:
        print(f"\n❌ Error saving file: {e}")
        return False

def main():
    """
    Main function
    """
    print("\n" + "=" * 80)
    print("🔧 MCX COMMODITY SECURITY ID FETCHER")
    print("=" * 80)
    print("This script will help you get Security IDs for MCX commodities")
    print("=" * 80 + "\n")
    
    # Download scrip master
    csv_content = download_scrip_master()
    if not csv_content:
        print("❌ Failed to download scrip master file")
        return
    
    # Parse MCX commodities
    commodities = parse_mcx_commodities(csv_content)
    
    # Display results
    display_commodities(commodities)
    
    # Generate code snippet
    if commodities:
        generate_code_snippet(commodities)
        save_to_file(commodities)
        
        print("\n✅ SUCCESS!")
        print("📝 Next steps:")
        print("   1. Copy the MCX_COMMODITIES code snippet above")
        print("   2. Paste it in your bot.py file")
        print("   3. Update your DhanHQ and Telegram credentials")
        print("   4. Run: python bot.py")
        print("=" * 80 + "\n")
    else:
        print("\n❌ No commodities found. Please check the scrip master file.")

if __name__ == "__main__":
    main()
