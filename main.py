"""
Fetch Current MCX Contracts from DhanHQ CSV
Finds active October/November 2025 contracts
Downloads: https://images.dhan.co/api-data/api-scrip-master.csv
"""

import requests
import csv
from datetime import datetime
from io import StringIO

def fetch_mcx_contracts():
    """Download and parse DhanHQ instruments CSV"""
    print("🔍 Downloading DhanHQ Instruments Master CSV...\n")
    
    try:
        # Download CSV
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        
        print(f"📡 Downloading: {url}")
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ Download successful! Size: {len(response.content)} bytes\n")
            
            # Parse CSV
            csv_text = response.content.decode('utf-8')
            csv_reader = csv.DictReader(StringIO(csv_text))
            
            # Target commodities
            target_commodities = {
                'GOLD': [],
                'GOLDM': [],
                'GOLDMINI': [],
                'GOLDPETAL': [],
                'SILVER': [],
                'SILVERM': [],
                'SILVERMICRO': [],
                'CRUDEOIL': [],
                'CRUDEOILM': [],
                'NATURALGAS': [],
                'COPPER': [],
                'ZINC': [],
                'ZINCMINI': [],
                'ALUMINIUM': [],
                'ALUMINIMINI': [],
                'LEAD': [],
                'LEADMINI': [],
                'NICKEL': [],
            }
            
            mcx_count = 0
            today = datetime.now()
            
            # October 2025 and November 2025
            target_months = ['2025-10', '2025-11', '2025-12']
            
            print("🔍 Filtering MCX Commodities...\n")
            
            for row in csv_reader:
                # Check if MCX exchange
                exchange = row.get('SEM_EXM_EXCH_ID', '')
                
                if exchange == 'MCX':
                    mcx_count += 1
                    
                    symbol = row.get('SEM_SMST_SECURITY_ID', '')
                    trading_symbol = row.get('SEM_TRADING_SYMBOL', '')
                    security_id = row.get('SEM_SECURITY_ID', '')
                    expiry = row.get('SEM_EXPIRY_DATE', '')
                    
                    # Check expiry date (should be in Oct/Nov/Dec 2025)
                    if expiry:
                        try:
                            expiry_date = datetime.strptime(expiry.split()[0], '%Y-%m-%d')
                            
                            # Skip expired
                            if expiry_date < today:
                                continue
                            
                            # Only Oct/Nov/Dec 2025
                            expiry_month = expiry[:7]  # YYYY-MM
                            if expiry_month not in target_months:
                                continue
                                
                        except:
                            continue
                    
                    # Match commodity names
                    symbol_upper = str(symbol).upper()
                    
                    for commodity in target_commodities.keys():
                        if symbol_upper.startswith(commodity):
                            target_commodities[commodity].append({
                                'security_id': security_id,
                                'symbol': symbol,
                                'trading_symbol': trading_symbol,
                                'expiry': expiry
                            })
                            break
            
            print(f"📊 Total MCX instruments: {mcx_count}")
            print(f"🔍 Filtered for Oct/Nov/Dec 2025 contracts\n")
            
            # Display results
            print("="*90)
            print("📊 CURRENT MCX CONTRACTS (October/November/December 2025)")
            print("="*90)
            
            updated_dict = {}
            
            for commodity, contracts in sorted(target_commodities.items()):
                if contracts:
                    print(f"\n🔹 {commodity}:")
                    
                    # Sort by expiry (nearest first)
                    contracts.sort(key=lambda x: x['expiry'] if x['expiry'] else 'Z')
                    
                    for i, contract in enumerate(contracts[:3], 1):  # Show top 3
                        print(f"   {i}. Security ID: {contract['security_id']:10s} | Expiry: {contract['expiry']:20s} | Symbol: {contract['trading_symbol']}")
                        
                        # Use nearest expiry (typically October 2025)
                        if i == 1:
                            updated_dict[int(contract['security_id'])] = commodity
            
            # Generate Python dictionary code
            print("\n" + "="*90)
            print("📝 UPDATED MCX_COMMODITIES DICTIONARY (Copy this to your code)")
            print("="*90)
            print("\nMCX_COMMODITIES = {")
            for sec_id, name in updated_dict.items():
                print(f'    {sec_id}: "{name}",')
            print("}\n")
            
            print("="*90)
            print(f"✅ Found {len(updated_dict)} active MCX contracts")
            print("="*90)
            
        else:
            print(f"❌ Download failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("\n" + "="*90)
    print("🚀 MCX INSTRUMENTS FETCHER - DhanHQ CSV Parser")
    print("="*90)
    print("Finding October/November 2025 active contracts...\n")
    
    fetch_mcx_contracts()
