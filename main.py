def main():
    """Main execution function"""
    
    # Load credentials from environment or config
    CLIENT_ID = os.getenv('DHAN_CLIENT_ID', 'YOUR_CLIENT_ID')
    ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', 'YOUR_ACCESS_TOKEN')
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
    if CLIENT_ID == 'YOUR_CLIENT_ID' or ACCESS_TOKEN == 'YOUR_ACCESS_TOKEN':
        print("=" * 60)
        print("MCX Trading System - DhanHQ API")"""
MCX Trading System using DhanHQ API
Complete system for MCX commodity trading with data fetching and analysis
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
from telegram_alerts import TelegramAlert

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
    
    # MCX Commodity Security IDs (Examples - update with actual IDs)
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
    
    # Exchange ID for MCX
    MCX = dhanhq.MCX
    
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
        self.dhan = dhanhq(client_id, access_token)
        
        # Initialize Telegram alerts
        self.telegram = None
        if telegram_bot_token and telegram_chat_id:
            self.telegram = TelegramAlert(telegram_bot_token, telegram_chat_id)
            logger.info("Telegram alerts enabled")
        else:
            logger.info("Telegram alerts disabled")
        
        logger.info("MCX Trading System initialized")
        
        # Verify connection
        self._verify_connection()
    
    def _verify_connection(self):
        """Verify API connection and token validity"""
        try:
            profile = self.dhan.get_profile()
            logger.info(f"Connected successfully. Client ID: {profile['dhanClientId']}")
            logger.info(f"Token validity: {profile['tokenValidity']}")
            logger.info(f"Active segments: {profile['activeSegment']}")
            return True
        except Exception as e:
            logger.error(f"Connection verification failed: {e}")
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
                return None
            
            response = self.dhan.get_ltp_data(
                self.MCX,
                security_id
            )
            
            if response and 'data' in response:
                ltp = response['data'].get('LTP')
                logger.info(f"{symbol} LTP: {ltp}")
                return ltp
            return None
        except Exception as e:
            logger.error(f"Error fetching LTP for {symbol}: {e}")
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
            
            response = self.dhan.get_quote_data(
                self.MCX,
                security_id
            )
            
            if response and 'data' in response:
                quote = response['data']
                logger.info(f"{symbol} Quote - LTP: {quote.get('LTP')}, "
                           f"Open: {quote.get('open')}, High: {quote.get('high')}, "
                           f"Low: {quote.get('low')}, Close: {quote.get('close')}")
                return quote
            return None
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol: str, interval: str = '1', 
                          from_date: str = None, to_date: str = None) -> Optional[pd.DataFrame]:
        """
        Get historical candle data for analysis
        
        Args:
            symbol: Commodity symbol
            interval: Time interval ('1', '5', '15', '25', '60', 'D')
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
            
            # Fetch historical data
            if interval == 'D':
                response = self.dhan.historical_daily_data(
                    security_id=str(security_id),
                    exchange_segment=self.MCX,
                    instrument_type=dhanhq.FUT,
                    from_date=from_date,
                    to_date=to_date
                )
            else:
                response = self.dhan.intraday_minute_data(
                    security_id=str(security_id),
                    exchange_segment=self.MCX,
                    instrument_type=dhanhq.FUT,
                    from_date=from_date,
                    to_date=to_date
                )
            
            if response and 'data' in response:
                df = pd.DataFrame(response['data'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                logger.info(f"Fetched {len(df)} candles for {symbol}")
                return df
            return None
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
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
            # Simple Moving Averages
            df['SMA_10'] = df['close'].rolling(window=10).mean()
            df['SMA_20'] = df['close'].rolling(window=20).mean()
            df['SMA_50'] = df['close'].rolling(window=50).mean()
            
            # Exponential Moving Averages
            df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
            df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()
            
            # MACD
            df['MACD'] = df['EMA_12'] - df['EMA_26']
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_Histogram'] = df['MACD'] - df['Signal']
            
            # RSI (Relative Strength Index)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # Bollinger Bands
            df['BB_Middle'] = df['close'].rolling(window=20).mean()
            bb_std = df['close'].rolling(window=20).std()
            df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
            df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
            
            # ATR (Average True Range)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df['ATR'] = true_range.rolling(14).mean()
            
            # Volume Moving Average
            df['Volume_SMA'] = df['volume'].rolling(window=20).mean()
            
            logger.info("Technical indicators calculated successfully")
            return df
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
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
            if df.empty or len(df) < 50:
                return {'signal': 'HOLD', 'reason': 'Insufficient data'}
            
            latest = df.iloc[-1]
            signals = []
            
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
            bullish = sum(1 for s in signals if 'Bullish' in s or 'Oversold' in s)
            bearish = sum(1 for s in signals if 'Bearish' in s or 'Overbought' in s)
            
            if bullish > bearish:
                overall_signal = 'BUY'
            elif bearish > bullish:
                overall_signal = 'SELL'
            else:
                overall_signal = 'HOLD'
            
            result = {
                'signal': overall_signal,
                'indicators': {
                    'price': latest['close'],
                    'SMA_10': round(latest['SMA_10'], 2),
                    'SMA_20': round(latest['SMA_20'], 2),
                    'RSI': round(latest['RSI'], 2),
                    'MACD': round(latest['MACD'], 4),
                    'ATR': round(latest['ATR'], 2)
                },
                'signals': signals
            }
            
            return result
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
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
        logger.info(f"Analyzing {symbol}...")
        
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
                self.telegram.send_trade_signal(
                    symbol,
                    signals['signal'],
                    signals['indicators'] | {'signals': signals['signals']}
                )
            
            return analysis
        else:
            return {
                'symbol': symbol,
                'error': 'Unable to fetch historical data'
            }
    
    def scan_all_commodities(self) -> List[Dict]:
        """
        Scan all configured commodities
        
        Returns:
            List of analysis results
        """
        results = []
        for symbol in self.COMMODITIES.keys():
            try:
                analysis = self.analyze_commodity(symbol)
                results.append(analysis)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")
        
        return results
    
    def get_positions(self) -> List[Dict]:
        """Get current positions"""
        try:
            positions = self.dhan.get_positions()
            logger.info(f"Retrieved {len(positions.get('data', []))} positions")
            return positions.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []
    
    def get_holdings(self) -> List[Dict]:
        """Get holdings"""
        try:
            holdings = self.dhan.get_holdings()
            logger.info(f"Retrieved {len(holdings.get('data', []))} holdings")
            return holdings.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching holdings: {e}")
            return []
    
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
            logger.info(f"Analysis saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")


def main():
    """Main execution function"""
    
    # Load credentials from environment or config
    CLIENT_ID = os.getenv('DHAN_CLIENT_ID', 'YOUR_CLIENT_ID')
    ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', 'YOUR_ACCESS_TOKEN')
    
    if CLIENT_ID == 'YOUR_CLIENT_ID' or ACCESS_TOKEN == 'YOUR_ACCESS_TOKEN':
        print("=" * 60)
        print("MCX Trading System - DhanHQ API")
        print("=" * 60)
        print("\nPlease set your credentials:")
        print("1. Set environment variables:")
        print("   export DHAN_CLIENT_ID='your_client_id'")
        print("   export DHAN_ACCESS_TOKEN='your_access_token'")
        print("\n2. Or edit this file and replace the default values")
        print("\nTo get your credentials:")
        print("   - Login to https://web.dhan.co")
        print("   - Go to My Profile > Access DhanHQ APIs")
        print("=" * 60)
        return
    
    try:
        # Initialize trading system
        system = MCXTradingSystem(CLIENT_ID, ACCESS_TOKEN)
        
        print("\n" + "=" * 60)
        print("MCX TRADING SYSTEM - MENU")
        print("=" * 60)
        print("1. Analyze single commodity")
        print("2. Scan all commodities")
        print("3. Get current positions")
        print("4. Get LTP for commodity")
        print("5. Exit")
        print("=" * 60)
        
        while True:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print("\nAvailable commodities:")
                for i, symbol in enumerate(system.COMMODITIES.keys(), 1):
                    print(f"{i}. {symbol}")
                symbol = input("\nEnter commodity symbol: ").strip().upper()
                
                if symbol in system.COMMODITIES:
                    analysis = system.analyze_commodity(symbol)
                    print(json.dumps(analysis, indent=2, default=str))
                    
                    save = input("\nSave analysis? (y/n): ").strip().lower()
                    if save == 'y':
                        system.save_analysis(analysis)
                else:
                    print("Invalid symbol!")
            
            elif choice == '2':
                print("\nScanning all commodities...")
                results = system.scan_all_commodities()
                print(json.dumps(results, indent=2, default=str))
                
                save = input("\nSave results? (y/n): ").strip().lower()
                if save == 'y':
                    system.save_analysis(results, 'mcx_scan_results.json')
            
            elif choice == '3':
                positions = system.get_positions()
                print(json.dumps(positions, indent=2, default=str))
            
            elif choice == '4':
                symbol = input("\nEnter commodity symbol: ").strip().upper()
                ltp = system.get_ltp(symbol)
                if ltp:
                    print(f"\n{symbol} LTP: ₹{ltp}")
            
            elif choice == '5':
                print("\nExiting... Thank you!")
                break
            
            else:
                print("Invalid choice!")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
