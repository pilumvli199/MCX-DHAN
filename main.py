"""
MCX Trading System using DhanHQ API (v2.0.2) with Telegram Alerts
Docker-compatible version with automated scanning
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dhanhq import dhanhq
import time
import logging
from typing import Dict, List, Optional

# Try to import Telegram alerts - make it optional
try:
    from telegram_alerts import TelegramAlert
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("⚠️  Warning: telegram_alerts module not found. Telegram features will be disabled.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcx_trading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MCXTradingSystem:
    """Main class for MCX commodity trading system"""
    
    # MCX Commodity Security IDs (Examples - verify with current IDs)
    COMMODITIES = {
        'GOLD': 112577,
        'SILVER': 112576,
        'CRUDEOIL': 26765,
        'NATURALGAS': 26761,
        'COPPER': 27165,
        'ZINC': 27159,
        'LEAD': 27161,
        'NICKEL': 27163
    }
    
    def __init__(self, client_id: str, access_token: str, 
                 telegram_bot_token: str = None, telegram_chat_id: str = None):
        """Initialize MCX Trading System"""
        self.client_id = client_id
        self.access_token = access_token
        
        try:
            self.dhan = dhanhq(client_id, access_token)
            logger.info("✓ DhanHQ client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize DhanHQ: {e}")
            raise
        
        # Initialize Telegram
        self.telegram = None
        if TELEGRAM_AVAILABLE and telegram_bot_token and telegram_chat_id:
            try:
                self.telegram = TelegramAlert(telegram_bot_token, telegram_chat_id)
                logger.info("✓ Telegram alerts enabled")
            except Exception as e:
                logger.warning(f"⚠ Telegram initialization failed: {e}")
        else:
            logger.info("Telegram alerts disabled")
        
        logger.info("MCX Trading System initialized")
        self._verify_connection()
    
    def _verify_connection(self):
        """Verify API connection"""
        try:
            logger.info("Verifying DhanHQ connection...")
            funds = self.dhan.get_fund_limits()
            if funds:
                logger.info("✓ Connected successfully to DhanHQ API")
                return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise
    
    def get_ltp(self, symbol: str) -> Optional[float]:
        """Get Last Traded Price"""
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                return None
            
            response = self.dhan.get_ltp_data(
                exchange_segment=dhanhq.MCX,
                security_id=str(security_id)
            )
            
            if response and 'data' in response:
                return response['data'].get('LTP')
            return None
        except Exception as e:
            logger.error(f"Error fetching LTP for {symbol}: {e}")
            return None
    
    def get_market_quote(self, symbol: str) -> Optional[Dict]:
        """Get market quote"""
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                return None
            
            response = self.dhan.marketfeed_data(
                exchange_segment=dhanhq.MCX,
                security_id=str(security_id)
            )
            
            return response.get('data') if response else None
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol: str, interval: str = 'D', 
                          from_date: str = None, to_date: str = None) -> Optional[pd.DataFrame]:
        """Get historical data"""
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                return None
            
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d')
            if not from_date:
                from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            if interval == 'D':
                response = self.dhan.historical_daily_data(
                    security_id=str(security_id),
                    exchange_segment=dhanhq.MCX,
                    instrument_type=dhanhq.FUT,
                    from_date=from_date,
                    to_date=to_date
                )
            else:
                response = self.dhan.intraday_minute_data(
                    security_id=str(security_id),
                    exchange_segment=dhanhq.MCX,
                    instrument_type=dhanhq.FUT,
                    interval=interval
                )
            
            if response and 'data' in response:
                df = pd.DataFrame(response['data'])
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp')
                    return df
            return None
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        try:
            if df is None or df.empty:
                return df
            
            # Moving Averages
            df['SMA_10'] = df['close'].rolling(window=10, min_periods=1).mean()
            df['SMA_20'] = df['close'].rolling(window=20, min_periods=1).mean()
            df['SMA_50'] = df['close'].rolling(window=50, min_periods=1).mean()
            
            # EMA
            df['EMA_12'] = df['close'].ewm(span=12, adjust=False, min_periods=1).mean()
            df['EMA_26'] = df['close'].ewm(span=26, adjust=False, min_periods=1).mean()
            
            # MACD
            df['MACD'] = df['EMA_12'] - df['EMA_26']
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False, min_periods=1).mean()
            df['MACD_Histogram'] = df['MACD'] - df['Signal']
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
            rs = gain / loss.replace(0, np.nan)
            df['RSI'] = 100 - (100 / (1 + rs))
            df['RSI'] = df['RSI'].fillna(50)
            
            # Bollinger Bands
            df['BB_Middle'] = df['close'].rolling(window=20, min_periods=1).mean()
            bb_std = df['close'].rolling(window=20, min_periods=1).std()
            df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
            df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
            
            # ATR
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            df['ATR'] = true_range.rolling(14, min_periods=1).mean()
            
            return df
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return df
    
    def generate_signals(self, df: pd.DataFrame) -> Dict:
        """Generate trading signals"""
        try:
            if df is None or df.empty or len(df) < 20:
                return {'signal': 'HOLD', 'reason': 'Insufficient data'}
            
            latest = df.iloc[-1]
            signals = []
            
            # Check indicators
            if latest['SMA_10'] > latest['SMA_20']:
                signals.append('Bullish: SMA10 > SMA20')
            elif latest['SMA_10'] < latest['SMA_20']:
                signals.append('Bearish: SMA10 < SMA20')
            
            if latest['MACD'] > latest['Signal']:
                signals.append('Bullish: MACD above Signal')
            elif latest['MACD'] < latest['Signal']:
                signals.append('Bearish: MACD below Signal')
            
            if latest['RSI'] < 30:
                signals.append('Oversold: RSI < 30')
            elif latest['RSI'] > 70:
                signals.append('Overbought: RSI > 70')
            
            if latest['close'] < latest['BB_Lower']:
                signals.append('Near Lower Band')
            elif latest['close'] > latest['BB_Upper']:
                signals.append('Near Upper Band')
            
            # Determine signal
            bullish = sum(1 for s in signals if 'Bullish' in s or 'Oversold' in s)
            bearish = sum(1 for s in signals if 'Bearish' in s or 'Overbought' in s)
            
            if bullish > bearish and bullish >= 2:
                overall_signal = 'BUY'
            elif bearish > bullish and bearish >= 2:
                overall_signal = 'SELL'
            else:
                overall_signal = 'HOLD'
            
            return {
                'signal': overall_signal,
                'indicators': {
                    'price': float(latest['close']),
                    'SMA_10': round(float(latest['SMA_10']), 2),
                    'SMA_20': round(float(latest['SMA_20']), 2),
                    'RSI': round(float(latest['RSI']), 2),
                    'MACD': round(float(latest['MACD']), 4),
                    'ATR': round(float(latest['ATR']), 2)
                },
                'signals': signals
            }
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return {'signal': 'HOLD', 'error': str(e)}
    
    def analyze_commodity(self, symbol: str, send_alert: bool = True) -> Dict:
        """Analyze commodity"""
        logger.info(f"🔍 Analyzing {symbol}...")
        
        quote = self.get_market_quote(symbol)
        df = self.get_historical_data(symbol, interval='D', 
                                      from_date=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
        
        if df is not None and not df.empty:
            df = self.calculate_technical_indicators(df)
            signals = self.generate_signals(df)
            
            analysis = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'current_quote': quote,
                'analysis': signals,
                'data_points': len(df)
            }
            
            # Send Telegram alert
            if send_alert and self.telegram and signals['signal'] != 'HOLD':
                try:
                    alert_data = signals['indicators'].copy()
                    alert_data['signals'] = signals['signals']
                    self.telegram.send_trade_signal(symbol, signals['signal'], alert_data)
                    logger.info(f"📱 Alert sent for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to send alert: {e}")
            
            logger.info(f"✓ {symbol}: {signals['signal']}")
            return analysis
        else:
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'error': 'No data available'
            }
    
    def scan_all_commodities(self, send_alerts: bool = True) -> List[Dict]:
        """Scan all commodities"""
        results = []
        logger.info("=" * 60)
        logger.info("🔍 STARTING COMMODITY SCAN")
        logger.info("=" * 60)
        
        for symbol in self.COMMODITIES.keys():
            try:
                analysis = self.analyze_commodity(symbol, send_alert=send_alerts)
                results.append(analysis)
                time.sleep(1)
            except Exception as e:
                logger.error(f"❌ Error analyzing {symbol}: {e}")
                results.append({'symbol': symbol, 'error': str(e)})
        
        logger.info("=" * 60)
        logger.info(f"✓ SCAN COMPLETED - {len(results)} commodities analyzed")
        logger.info("=" * 60)
        return results
    
    def save_analysis(self, analysis: Dict, filename: str = None):
        """Save analysis to file"""
        if not filename:
            filename = f"mcx_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            logger.info(f"✓ Analysis saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving: {e}")


def run_automated_scan(system: MCXTradingSystem, mode: str = 'all'):
    """Run automated scan - Docker compatible"""
    logger.info("\n" + "=" * 60)
    logger.info("🤖 AUTOMATED SCAN MODE")
    logger.info("=" * 60)
    
    if mode == 'all':
        # Scan all commodities with alerts
        results = system.scan_all_commodities(send_alerts=True)
        system.save_analysis(results, 'mcx_scan_all.json')
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 SCAN SUMMARY")
        print("=" * 60)
        
        buy_signals = [r for r in results if r.get('analysis', {}).get('signal') == 'BUY']
        sell_signals = [r for r in results if r.get('analysis', {}).get('signal') == 'SELL']
        hold_signals = [r for r in results if r.get('analysis', {}).get('signal') == 'HOLD']
        
        print(f"\n🟢 BUY Signals: {len(buy_signals)}")
        for r in buy_signals:
            print(f"   • {r['symbol']}")
        
        print(f"\n🔴 SELL Signals: {len(sell_signals)}")
        for r in sell_signals:
            print(f"   • {r['symbol']}")
        
        print(f"\n🟡 HOLD Signals: {len(hold_signals)}")
        
        print("\n" + "=" * 60)
        print(f"📁 Results saved to: mcx_scan_all.json")
        print("=" * 60 + "\n")
        
    elif mode == 'single':
        # Analyze single commodity (GOLD by default)
        symbol = os.getenv('ANALYZE_SYMBOL', 'GOLD')
        logger.info(f"Analyzing {symbol}...")
        analysis = system.analyze_commodity(symbol, send_alert=True)
        system.save_analysis(analysis, f'mcx_{symbol.lower()}_analysis.json')
        print(json.dumps(analysis, indent=2, default=str))


def main():
    """Main function - Docker compatible"""
    
    # Get credentials
    CLIENT_ID = os.getenv('DHAN_CLIENT_ID', 'YOUR_CLIENT_ID')
    ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', 'YOUR_ACCESS_TOKEN')
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
    # Get mode
    MODE = os.getenv('SCAN_MODE', 'all')  # 'all' or 'single'
    
    if CLIENT_ID == 'YOUR_CLIENT_ID' or ACCESS_TOKEN == 'YOUR_ACCESS_TOKEN':
        print("\n" + "=" * 60)
        print("❌ CREDENTIALS NOT SET")
        print("=" * 60)
        print("\nSet environment variables:")
        print("  export DHAN_CLIENT_ID='your_client_id'")
        print("  export DHAN_ACCESS_TOKEN='your_access_token'")
        print("\nOptional:")
        print("  export TELEGRAM_BOT_TOKEN='your_token'")
        print("  export TELEGRAM_CHAT_ID='your_chat_id'")
        print("  export SCAN_MODE='all'  # or 'single'")
        print("  export ANALYZE_SYMBOL='GOLD'  # for single mode")
        print("=" * 60 + "\n")
        return
    
    try:
        # Initialize system
        print("\n" + "=" * 60)
        print("🚀 MCX TRADING SYSTEM - DOCKER MODE")
        print("=" * 60 + "\n")
        
        system = MCXTradingSystem(
            CLIENT_ID, 
            ACCESS_TOKEN,
            TELEGRAM_BOT_TOKEN if TELEGRAM_BOT_TOKEN else None,
            TELEGRAM_CHAT_ID if TELEGRAM_CHAT_ID else None
        )
        
        # Run automated scan
        run_automated_scan(system, mode=MODE)
        
        logger.info("✓ Execution completed successfully")
        
    except KeyboardInterrupt:
        logger.info("⚠️  Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
