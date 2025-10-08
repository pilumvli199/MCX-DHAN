"""
Fetch Current MCX Contracts from DhanHQ Instruments Master
Finds active October/November 2025 contracts
"""

import requests
import json
from datetime import datetime
import os

# DhanHQ API Credentials
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")

DHAN_API_BASE = "https://api.dhan.co/v2"

def fetch_instruments():
    """Fetch MCX instruments master data"""
    print("🔍 Fetching MCX Instruments Master Data...\n")
    
    try:
        # DhanHQ Instruments API
        url = f"{DHAN_API_BASE}/instruments"
        
        headers = {
            "access-token": DHAN_ACCESS_TOKEN,
            "Content-Type": "application/json"
        }
        
        print(f"📡 Requesting: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        
        print(f"✅ Response Status: {response.status_code}\n")
        
        if response.status_code == 200:
            data = response.json()
            
            # Filter MCX commodities
            print("🔍 Filtering MCX Commodities...\n")
            
            if isinstance(data, list):
                mcx_instruments = [item for item in data if item.get('SEM_EXM_EXCH_ID') == 'MCX' or item.get('exchange') == 'MCX']
            elif 'data' in data:
                mcx_instruments = [item for item in data['data'] if item.get('SEM_EXM_EXCH_ID') == 'MCX' or item.get('exchange') == 'MCX']
            else:
                print("⚠️ Unexpected data format")
                print(json.dumps(data, indent=2)[:500])
                return
            
            print(f"📊 Found {len(mcx_instruments)} MCX instruments\n")
            
            # Target commodities
            target_commodities = {
                'GOLD': [],
                'GOLDM': [],
                'SILVER': [],
                'SILVERM': [],
                'CRUDEOIL': [],
                'NATURALGAS': [],
                'COPPER': [],
                'ZINC': [],
                'ALUMINIUM': [],
                'LEAD': [],
                'NICKEL': []
            }
            
            # Current date for expiry comparison
            today = datetime.now()
            
            # Filter active contracts
            for instrument in mcx_instruments:
                symbol = instrument.get('SEM_SMST_SECURITY_ID') or instrument.get('symbol', '')
                trading_symbol = instrument.get('SEM_TRADING_SYMBOL') or instrument.get('tradingSymbol', '')
                security_id = instrument.get('SEM_SECURITY_ID') or instrument.get('securityId', '')
                expiry = instrument.get('SEM_EXPIRY_DATE') or instrument.get('expiryDate', '')
                
                # Check expiry date
                if expiry:
                    try:
                        if isinstance(expiry, str):
                            expiry_date = datetime.strptime(expiry.split()[0], '%Y-%m-%d')
                        else:
                            expiry_date = expiry
                        
                        # Only future contracts (not expired)
                        if expiry_date < today:
                            continue
                    except:
                        pass
                
                # Match commodity names
                symbol_upper = str(symbol).upper()
                trading_upper = str(trading_symbol).upper()
                
                for commodity in target_commodities.keys():
                    if commodity in symbol_upper or commodity in trading_upper:
                        target_commodities[commodity].append({
                            'security_id': security_id,
                            'symbol': symbol,
                            'trading_symbol': trading_symbol,
                            'expiry': expiry
                        })
            
            # Display results
            print("="*80)
            print("📊 CURRENT MCX CONTRACTS (Active/Future)")
            print("="*80)
            
            updated_dict = {}
            
            for commodity, contracts in target_commodities.items():
                if contracts:
                    print(f"\n🔹 {commodity}:")
                    
                    # Sort by expiry (nearest first)
                    contracts.sort(key=lambda x: x['expiry'] if x['expiry'] else 'Z')
                    
                    for i, contract in enumerate(contracts[:3], 1):  # Show top 3
                        print(f"   {i}. Security ID: {contract['security_id']}")
                        print(f"      Symbol: {contract['trading_symbol']}")
                        print(f"      Expiry: {contract['expiry']}")
                        
                        # Use nearest expiry for main dict
                        if i == 1:
                            updated_dict[contract['security_id']] = commodity
                else:
                    print(f"\n⚠️ {commodity}: No active contracts found")
            
            # Generate Python dictionary code
            print("\n" + "="*80)
            print("📝 UPDATED MCX_COMMODITIES DICTIONARY")
            print("="*80)
            print("\nMCX_COMMODITIES = {")
            for sec_id, name in updated_dict.items():
                print(f'    {sec_id}: "{name}",')
            print("}")
            
            # Save to file
            with open('mcx_current_contracts.json', 'w') as f:
                json.dump({
                    'updated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'contracts': updated_dict,
                    'full_data': target_commodities
                }, f, indent=2)
            
            print("\n✅ Saved to: mcx_current_contracts.json")
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 MCX INSTRUMENTS FETCHER - DhanHQ")
    print("="*80)
    print("Finding current month active contracts...\n")
    
    if DHAN_CLIENT_ID == "YOUR_DHAN_CLIENT_ID":
        print("❌ ERROR: Set DHAN_CLIENT_ID environment variable!")
    elif DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN":
        print("❌ ERROR: Set DHAN_ACCESS_TOKEN environment variable!")
    else:
        fetch_instruments()
    
    print("\n" + "="*80)
