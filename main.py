"""
MCX Trading System using DhanHQ API (v2.0.2) with Telegram Alerts
Complete system for MCX commodity trading with data fetching and analysis
DEBUGGED VERSION - Fixed all issues
"""

import os
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
    print("   Install: pip install requests")
    print("   And ensure telegram_alerts.py is in the same directory.\n")

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
        'GOLD': 112577,      # Gold futures
        'SILVER': 112576,    # Silver futures
        'CRUDEOIL': 26765,   # Crude Oil
        'NATURALGAS': 26761, # Natural Gas
        'COPPER': 27165,     # Copper
        'ZINC': 27159,       # Zinc
        'LEAD': 27161,       # Lead
        'NICKEL': 27163      # Nickel
    }
    
    def __init__(self, client_id: str, access_token: str, 
                 telegram_bot_token: str = None, telegram_chat_id: str = None):
        """
        Initialize MCX Trading System
        
        Args:
            client_id: DhanHQ client ID
            access_token: DhanHQ access token
            telegram_bot_token: Telegram bot token (optional)
            telegram_chat_id: Telegram chat ID (optional)
        """
        self.client_id = client_id
        self.access_token = access_token
        
        # Initialize DhanHQ
        try:
            self.dhan = dhanhq(client_id, access_token)
            logger.info("✓ DhanHQ client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize DhanHQ: {e}")
            raise
        
        # Initialize Telegram alerts
        self.telegram = None
        if TELEGRAM_AVAILABLE and telegram_bot_token and telegram_chat_id:
            try:
                self.telegram = TelegramAlert(telegram_bot_token, telegram_chat_id)
                logger.info("✓ Telegram alerts enabled")
            except Exception as e:
                logger.warning(f"⚠ Telegram initialization failed: {e}")
                logger.info("Continuing without Telegram alerts...")
        else:
            if not TELEGRAM_AVAILABLE:
                logger.info("Telegram module not available")
            else:
                logger.info("Telegram alerts disabled (no credentials provided)")
        
        logger.info("MCX Trading System initialized with dhanhq v2.0.2")
        
        # Verify connection
        self._verify_connection()
    
    def _verify_connection(self):
        """Verify API connection and token validity"""
        try:
            # In v2.0.2, use get_fund_limits() to verify connection
            logger.info("Verifying connection to DhanHQ...")
            funds = self.dhan.get_fund_limits()
            if funds:
                logger.info("✓ Connected successfully to DhanHQ API")
                return True
        except Exception as e:
            logger.error(f"❌ Connection verification failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            logger.warning("Please ensure your CLIENT_ID and ACCESS_TOKEN are correct")
            logger.warning("Visit: https://web.dhan.co > My Profile > Access DhanHQ APIs")
            raise
    
    def get_ltp(self, symbol: str) -> Optional[float]:
        """
        Get Last Traded Price for a commodity
        
        Args:
            symbol: Commodity symbol (e.g., 'GOLD', 'SILVER')
        
        Returns:
            Last traded price or None
        """
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                logger.error(f"Invalid symbol: {symbol}")
                logger.info(f"Available symbols: {', '.join(self.COMMODITIES.keys())}")
                return None
            
            # Updated for v2.0.2 - MCX exchange segment
            logger.info(f"Fetching LTP for {symbol} (ID: {security_id})...")
            response = self.dhan.get_ltp_data(
                exchange_segment=dhanhq.MCX,
                security_id=str(security_id)
            )
            
            if response and 'data' in response:
                ltp = response['data'].get('LTP')
                if ltp:
                    logger.info(f"✓ {symbol} LTP: ₹{ltp}")
                    return ltp
                else:
                    logger.warning(f"No LTP data in response for {symbol}")
            else:
                logger.warning(f"Invalid response format for {symbol}: {response}")
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching LTP for {symbol}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return None
    
    def get_market_quote(self, symbol: str) -> Optional[Dict]:
        """
        Get detailed market quote for a commodity
        
        Args:
            symbol: Commodity symbol
        
        Returns:
            Market quote data dictionary
        """
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                logger.error(f"Invalid symbol: {symbol}")
                return None
            
            logger.info(f"Fetching market quote for {symbol}...")
            response = self.dhan.marketfeed_data(
                exchange_segment=dhanhq.MCX,
                security_id=str(security_id)
            )
            
            if response and 'data' in response:
                quote = response['data']
                logger.info(f"✓ {symbol} Quote - LTP: {quote.get('LTP')}, "
                           f"Open: {quote.get('open')}, High: {quote.get('high')}, "
                           f"Low: {quote.get('low')}, Close: {quote.get('prev_close')}")
                return quote
            else:
                logger.warning(f"Invalid quote response for {symbol}")
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching quote for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol: str, interval: str = 'D', 
                          from_date: str = None, to_date: str = None) -> Optional[pd.DataFrame]:
        """
        Get historical candle data for analysis
        
        Args:
            symbol: Commodity symbol
            interval: Time interval ('1', '5', '15', '60', 'D')
                     1 = 1 minute, 5 = 5 minutes, D = Daily
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with OHLCV data
        """
        try:
            security_id = self.COMMODITIES.get(symbol.upper())
            if not security_id:
                logger.error(f"Invalid symbol: {symbol}")
                return None
            
            # Set default dates if not provided
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d')
            if not from_date:
                from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            logger.info(f"Fetching historical data for {symbol} ({interval} interval, {from_date} to {to_date})...")
            
            # Fetch historical data based on interval
            response = None
            if interval == 'D':
                # Daily data
                response = self.dhan.historical_daily_data(
                    security_id=str(security_id),
                    exchange_segment=dhanhq.MCX,
                    instrument_type=dhanhq.FUT,
                    from_date=from_date,
                    to_date=to_date
                )
            else:
                # Intraday minute data
                response = self.dhan.intraday_minute_data(
                    security_id=str(security_id),
                    exchange_segment=dhanhq.MCX,
                    instrument_type=dhanhq.FUT,
                    interval=interval
                )
            
            if response and isinstance(response, dict) and 'data' in response:
                df = pd.DataFrame(response['data'])
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp')
                    logger.info(f"✓ Fetched {len(df)} candles for {symbol}")
                    return df
                else:
                    logger.warning(f"Empty dataframe received for {symbol}")
            else:
                logger.warning(f"Invalid historical data response for {symbol}")
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching historical data for {symbol}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return None
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators for analysis
        
        Args:
            df: DataFrame with OHLCV data
        
        Returns:
            DataFrame with added indicators
        """
        try:
            if df is None or df.empty:
                logger.warning("Cannot calculate indicators - empty dataframe")
                return df
            
            logger.info("Calculating technical indicators...")
            
            # Simple Moving Averages
            df['SMA_10'] = df['close'].rolling(window=10, min_periods=1).mean()
            df['SMA_20'] = df['close'].rolling(window=20, min_periods=1).mean()
            df['SMA_50'] = df['close'].rolling(window=50, min_periods=1).mean()
            
            # Exponential Moving Averages
            df['EMA_12'] = df['close'].ewm(span=12, adjust=False, min_periods=1).mean()
            df['EMA_26'] = df['close'].ewm(span=26, adjust=False, min_periods=1).mean()
            
            # MACD
            df['MACD'] = df['EMA_12'] - df['EMA_26']
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False, min_periods=1).mean()
            df['MACD_Histogram'] = df['MACD'] - df['Signal']
            
            # RSI (Relative Strength Index)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
            rs = gain / loss.replace(0, np.nan)
            df['RSI'] = 100 - (100 / (1 + rs))
            df['RSI'] = df['RSI'].fillna(50)  # Fill NaN with neutral value
            
            # Bollinger Bands
            df['BB_Middle'] = df['close'].rolling(window=20, min_periods=1).mean()
            bb_std = df['close'].rolling(window=20, min_periods=1).std()
            df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
            df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
            
            # ATR (Average True Range)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            df['ATR'] = true_range.rolling(14, min_periods=1).mean()
            
            # Volume Moving Average
            if 'volume' in df.columns:
                df['Volume_SMA'] = df['volume'].rolling(window=20, min_periods=1).mean()
            
            logger.info("✓ Technical indicators calculated successfully")
            return df
        except Exception as e:
            logger.error(f"❌ Error calculating indicators: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return df
    
    def generate_signals(self, df: pd.DataFrame) -> Dict:
        """
        Generate trading signals based on indicators
        
        Args:
            df: DataFrame with indicators
        
        Returns:
            Dictionary with signals and analysis
        """
        try:
            if df is None or df.empty or len(df) < 20:
                logger.warning("Insufficient data for signal generation")
                return {'signal': 'HOLD', 'reason': 'Insufficient data'}
            
            latest = df.iloc[-1]
            signals = []
            
            # Check if all required columns exist
            required_cols = ['SMA_10', 'SMA_20', 'MACD', 'Signal', 'MACD_Histogram', 
                           'RSI', 'close', 'BB_Lower', 'BB_Upper']
            missing_cols = [col for col in required_cols if col not in df.columns or pd.isna(latest[col])]
            
            if missing_cols:
                logger.warning(f"Missing or NaN indicators: {missing_cols}")
                return {
                    'signal': 'HOLD',
                    'reason': f'Missing indicators: {missing_cols}',
                    'indicators': {
                        'price': latest.get('close', 0),
                    }
                }
            
            # Moving Average Crossover
            if latest['SMA_10'] > latest['SMA_20']:
                signals.append('Bullish: SMA10 > SMA20')
            elif latest['SMA_10'] < latest['SMA_20']:
                signals.append('Bearish: SMA10 < SMA20')
            
            # MACD
            if latest['MACD'] > latest['Signal'] and latest['MACD_Histogram'] > 0:
                signals.append('Bullish: MACD above Signal')
            elif latest['MACD'] < latest['Signal'] and latest['MACD_Histogram'] < 0:
                signals.append('Bearish: MACD below Signal')
            
            # RSI
            if latest['RSI'] < 30:
                signals.append('Oversold: RSI < 30')
            elif latest['RSI'] > 70:
                signals.append('Overbought: RSI > 70')
            
            # Bollinger Bands
            if latest['close'] < latest['BB_Lower']:
                signals.append('Near Lower Band: Potential bounce')
            elif latest['close'] > latest['BB_Upper']:
                signals.append('Near Upper Band: Potential reversal')
            
            # Determine overall signal
            bullish = sum(1 for s in signals if 'Bullish' in s or 'Oversold' in s or 'bounce' in s)
            bearish = sum(1 for s in signals if 'Bearish' in s or 'Overbought' in s or 'reversal' in s)
            
            if bullish > bearish and bullish >= 2:
                overall_signal = 'BUY'
            elif bearish > bullish and bearish >= 2:
                overall_signal = 'SELL'
            else:
                overall_signal = 'HOLD'
            
            result = {
                'signal': overall_signal,
                'indicators': {
                    'price': float(latest['close']),
                    'SMA_10': round(float(latest['SMA_10']), 2),
                    'SMA_20': round(float(latest['SMA_20']), 2),
                    'RSI': round(float(latest['RSI']), 2),
                    'MACD': round(float(latest['MACD']), 4),
                    'ATR': round(float(latest['ATR']), 2)
                },
                'signals': signals,
                'bullish_count': bullish,
                'bearish_count': bearish
            }
            
            logger.info(f"✓ Signal generated: {overall_signal} (Bullish: {bullish}, Bearish: {bearish})")
            return result
        except Exception as e:
            logger.error(f"❌ Error generating signals: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return {'signal': 'HOLD', 'error': str(e)}
    
    def analyze_commodity(self, symbol: str, send_alert: bool = True) -> Dict:
        """
        Complete analysis for a commodity
        
        Args:
            symbol: Commodity symbol
            send_alert: Send Telegram alert if enabled
        
        Returns:
            Complete analysis dictionary
        """
        logger.info(f"🔍 Analyzing {symbol}...")
        
        # Get current market data
        quote = self.get_market_quote(symbol)
        
        # Get historical data
        df = self.get_historical_data(symbol, interval='D', 
                                      from_date=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
        
        if df is not None and not df.empty:
            # Calculate indicators
            df = self.calculate_technical_indicators(df)
            
            # Generate signals
            signals = self.generate_signals(df)
            
            analysis = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'current_quote': quote,
                'analysis': signals,
                'data_points': len(df)
            }
            
            # Send Telegram alert if enabled and signal is not HOLD
            if send_alert and self.telegram and signals['signal'] != 'HOLD':
                logger.info(f"📱 Sending Telegram alert for {symbol} - {signals['signal']}")
                try:
                    # Combine indicators with signals list
                    alert_data = signals['indicators'].copy()
                    alert_data['signals'] = signals['signals']
                    
                    self.telegram.send_trade_signal(
                        symbol,
                        signals['signal'],
                        alert_data
                    )
                except Exception as e:
                    logger.error(f"Failed to send Telegram alert: {e}")
            
            logger.info(f"✓ Analysis completed for {symbol}")
            return analysis
        else:
            logger.warning(f"Unable to complete analysis for {symbol} - no historical data")
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'error': 'Unable to fetch historical data'
            }
    
    def scan_all_commodities(self, send_alerts: bool = True) -> List[Dict]:
        """
        Scan all configured commodities
        
        Args:
            send_alerts: Send Telegram alerts for signals
        
        Returns:
            List of analysis results
        """
        results = []
        logger.info("🔍 Starting commodity scan...")
        
        for symbol in self.COMMODITIES.keys():
            try:
                logger.info(f"\n{'='*60}")
                analysis = self.analyze_commodity(symbol, send_alert=send_alerts)
                results.append(analysis)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"❌ Error analyzing {symbol}: {e}")
                results.append({
                    'symbol': symbol,
                    'error': str(e)
                })
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ Scan completed. Analyzed {len(results)} commodities")
        return results
    
    def get_positions(self) -> List[Dict]:
        """Get current positions"""
        try:
            logger.info("Fetching current positions...")
            positions = self.dhan.get_positions()
            if positions and 'data' in positions:
                logger.info(f"✓ Retrieved {len(positions['data'])} positions")
                return positions['data']
            else:
                logger.info("No positions found")
                return []
        except Exception as e:
            logger.error(f"❌ Error fetching positions: {e}")
            return []
    
    def get_fund_limits(self) -> Dict:
        """Get fund limits and available margin"""
        try:
            logger.info("Fetching fund limits...")
            funds = self.dhan.get_fund_limits()
            logger.info("✓ Retrieved fund limits")
            return funds
        except Exception as e:
            logger.error(f"❌ Error fetching fund limits: {e}")
            return {}
    
    def save_analysis(self, analysis: Dict, filename: str = None):
        """
        Save analysis to JSON file
        
        Args:
            analysis: Analysis data
            filename: Output filename
        """
        if not filename:
            filename = f"mcx_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            logger.info(f"✓ Analysis saved to {filename}")
        except Exception as e:
            logger.error(f"❌ Error saving analysis: {e}")


def main():
    """Main execution function"""
    
    # Load credentials from environment or config
    CLIENT_ID = os.getenv('DHAN_CLIENT_ID', 'YOUR_CLIENT_ID')
    ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', 'YOUR_ACCESS_TOKEN')
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
    if CLIENT_ID == 'YOUR_CLIENT_ID' or ACCESS_TOKEN == 'YOUR_ACCESS_TOKEN':
        print("\n" + "=" * 60)
        print("MCX Trading System - DhanHQ API v2.0.2")
        print("=" * 60)
        print("\n❌ Please set your credentials:")
        print("\n1. Set environment variables:")
        print("   export DHAN_CLIENT_ID='your_client_id'")
        print("   export DHAN_ACCESS_TOKEN='your_access_token'")
        print("\n2. Optional - For Telegram alerts:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        print("\nTo get your credentials:")
        print("   - Dhan: Login to https://web.dhan.co")
        print("   - Go to: My Profile > Access DhanHQ APIs")
        print("   - Telegram: Chat with @BotFather to create bot")
        print("   - Get Chat ID from @userinfobot")
        print("=" * 60 + "\n")
        return
    
    try:
        # Initialize trading system
        print("\n" + "=" * 60)
        print("Initializing MCX Trading System...")
        print("=" * 60 + "\n")
        
        system = MCXTradingSystem(
            CLIENT_ID, 
            ACCESS_TOKEN,
            TELEGRAM_BOT_TOKEN if TELEGRAM_BOT_TOKEN else None,
            TELEGRAM_CHAT_ID if TELEGRAM_CHAT_ID else None
        )
        
        print("\n" + "=" * 60)
        print("📊 MCX TRADING SYSTEM - MENU 📊")
        print("=" * 60)
        
        # Show Telegram status
        if system.telegram:
            print("✅ Telegram Alerts: ENABLED")
        else:
            print("⚠️  Telegram Alerts: DISABLED")
        
        print("\n1. Analyze single commodity")
        print("2. Scan all commodities (with alerts)")
        print("3. Scan all commodities (no alerts)")
        print("4. Get current positions")
        print("5. Get LTP for commodity")
        print("6. Get fund limits")
        print("7. Test Telegram alert")
        print("8. Exit")
        print("=" * 60)
        
        while True:
            choice = input("\n👉 Enter your choice (1-8): ").strip()
            
            if choice == '1':
                print("\n📋 Available commodities:")
                for i, symbol in enumerate(system.COMMODITIES.keys(), 1):
                    print(f"{i}. {symbol}")
                symbol = input("\n💰 Enter commodity symbol: ").strip().upper()
                
                if symbol in system.COMMODITIES:
                    send_alert = input("📱 Send Telegram alert? (y/n): ").strip().lower() == 'y'
                    analysis = system.analyze_commodity(symbol, send_alert=send_alert)
                    print("\n" + "=" * 60)
                    print(json.dumps(analysis, indent=2, default=str))
                    print("=" * 60)
                    
                    save = input("\n💾 Save analysis? (y/n): ").strip().lower()
                    if save == 'y':
                        system.save_analysis(analysis)
                else:
                    print("❌ Invalid symbol!")
            
            elif choice == '2':
                print("\n🔍 Scanning all commodities (WITH alerts)...")
                results = system.scan_all_commodities(send_alerts=True)
                print("\n" + "=" * 60)
                print(json.dumps(results, indent=2, default=str))
                print("=" * 60)
                
                save = input("\n💾 Save results? (y/n): ").strip().lower()
                if save == 'y':
                    system.save_analysis(results, 'mcx_scan_results.json')
            
            elif choice == '3':
                print("\n🔍 Scanning all commodities (NO alerts)...")
                results = system.scan_all_commodities(send_alerts=False)
                print("\n" + "=" * 60)
                print(json.dumps(results, indent=2, default=str))
                print("=" * 60)
                
                save = input("\n💾 Save results? (y/n): ").strip().lower()
                if save == 'y':
                    system.save_analysis(results, 'mcx_scan_results.json')
            
            elif choice == '4':
                print("\n📈 Fetching positions...")
                positions = system.get_positions()
                print("\n" + "=" * 60)
                print(json.dumps(positions, indent=2, default=str))
                print("=" * 60)
            
            elif choice == '5':
                symbol = input("\n💰 Enter commodity symbol: ").strip().upper()
                ltp = system.get_ltp(symbol)
                if ltp:
                    print(f"\n✅ {symbol} LTP: ₹{ltp}")
            
            elif choice == '6':
                print("\n💵 Fetching fund limits...")
                funds = system.get_fund_limits()
                print("\n" + "=" * 60)
                print(json.dumps(funds, indent=2, default=str))
                print("=" * 60)
            
            elif choice == '7':
                if system.telegram:
                    print("\n📱 Sending test alert...")
                    test_data = {
                        'price': 62500.00,
                        'SMA_10': 62400.00,
                        'SMA_20': 62300.00,
                        'RSI': 55.5,
                        'MACD': 0.0025,
                        'ATR': 150.00,
                        'signals': [
                            'Bullish: SMA10 > SMA20',
                            'MACD above Signal'
                        ]
                    }
                    success = system.telegram.send_trade_signal('GOLD', 'BUY', test_data)
                    if success:
                        print("✅ Test alert sent successfully!")
                    else:
                        print("❌ Failed to send test alert. Check logs.")
                else:
                    print("❌ Telegram alerts not enabled!")
                    print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
            
            elif choice == '8':
                print("\n👋 Exiting... धन्यवाद! (Thank you!)")
                break
            
            else:
                print("❌ Invalid choice!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user")
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()
