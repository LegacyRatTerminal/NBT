import time
import numpy as np
import talib
from telegram.ext import Application
from telegram.constants import ParseMode
import asyncio
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime, timedelta
from tabulate import tabulate
import os
from dotenv import load_dotenv
import logging
import logging
import sys
import json
from typing import Dict, List, Optional, Tuple
import pandas as pd

def setup_logging():
    """Configure logging with proper encoding handling"""
    # Force UTF-8 encoding for StreamHandler
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    
    # Create formatters and handlers
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler (using UTF-8)
    file_handler = logging.FileHandler('trading_bot.log', encoding='utf-8')
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    
    # Setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class Config:
   
    # API Credentials
    BINANCE_API_KEY = '5UnHUp24PBWhfG43cJgMqkWUYVMUkJjJMBOWjpjCz3BLUEIzdQJnK6MaBb5X3anp'
    BINANCE_API_SECRET = 'kLIELdxpfyG33G50lEFo8XbwGy6gaixI0BOp2R3KWPEPHen2xKdF6iuHFFWIlmPQ'
    TELEGRAM_BOT_TOKEN = '7892769491:AAEVZuT2pQmL6034tfUt8EiV1DLDDX-XxCw'
    TELEGRAM_CHAT_ID = '7917402606'

    # Trading parameters
    SYMBOLS = ['BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'LINKUSDT', 'AAVEUSDT', 'ADAUSDT', 'UNIUSDT', 'TRXUSDT', 'AGLDUSDT']
    TIMEFRAMES = ['5m', '15m']  
    # Indicator parameters
    RSI_PERIOD = 14
    RSI_OVERSOLD = 24  # Relaxed from 35
    RSI_OVERBOUGHT = 76  # Relaxed from 65
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    
    # Risk management
    TRADE_AMOUNT = 0.8  # 40% of balance
    MAX_OPEN_TRADES = 1
    MAX_STOP_LOSS = 0.5 # 1%
    MAX_PROFIT = 0.6  # 2%
    MAX_LEVERAGE = 20

    # Enhanced trailing stop settings
    INITIAL_TRAILING_STOP = 1.0  # Initial trailing stop percentage (wider)
    MIN_TRAILING_STOP = 0.2      # Minimum trailing stop as price moves in favor
    TRAILING_ACTIVATION = 0.4    # % profit needed before tightening trailing stop
    TIGHTENING_STEP = 0.1       # How much to tighten trail stop as profit increases
    
    # Market conditions
    MIN_VOLUME_24H = 400000  # Minimum 24h volume in USDT
    MAX_SPREAD_PERCENT = 0.7  # Maximum allowed spread (0.1%)
    
class TradingStats:
    def __init__(self):
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.trade_history: List[Dict] = []
        
    def add_trade(self, trade_data: Dict):
        """Record trade data and update statistics"""
        self.trade_history.append(trade_data)
        self.total_trades += 1
        
        if trade_data['pnl'] > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
            
    def get_win_rate(self) -> float:
        """Calculate current win rate"""
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

class TradingBot:
    def __init__(self):
        """Initialize the TradingBot with enhanced features"""
        try:
            self._validate_config()
            self.client = Client(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET)
            self._init_telegram()
            
            self.open_trades: Dict = {}
            self.running = False
            self.stats = TradingStats()
            self.symbol_data: Dict = {}
            
            self._setup_trading_environment()
            self._load_trading_history()
            
            logger.info("Bot initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Critical error during initialization: {e}", exc_info=True)
            raise

    def _validate_config(self):
        """Validate configuration parameters"""
        required_env_vars = ['BINANCE_API_KEY', 'BINANCE_API_SECRET', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
        missing_vars = [var for var in required_env_vars if not getattr(Config, var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _setup_trading_environment(self):
        """Setup trading environment and validate symbols"""
        logger.info("Setting up trading environment...")
        
        # Validate and setup symbols
        for symbol in Config.SYMBOLS:
            try:
                # Get symbol information
                info = self.client.futures_exchange_info()
                symbol_info = next((item for item in info['symbols'] if item['symbol'] == symbol), None)
                
                if not symbol_info:
                    logger.warning(f"Symbol {symbol} not found in futures exchange")
                    continue
                
                try:
                    # Set leverage
                    self.client.futures_change_leverage(
                        symbol=symbol,
                        leverage=Config.MAX_LEVERAGE
                    )
                except Exception as e:
                    logger.error(f"Error setting leverage for {symbol}: {e}")
                    continue
                
                # Store symbol information
                self.symbol_data[symbol] = {
                    'info': symbol_info,
                    'filters': {f['filterType']: f for f in symbol_info['filters']},
                    'precision': {
                        'price': symbol_info['pricePrecision'],
                        'quantity': symbol_info['quantityPrecision']
                    }
                }
                
                # Use plain text instead of Unicode checkmark
                logger.info(f"Setup completed for {symbol}")
                
            except Exception as e:
                logger.error(f"Error setting up {symbol}: {e}")
                
        # Verify account
        try:
            account = self.client.futures_account()
            balance = float(account['totalWalletBalance'])
            logger.info(f"Account connected. Balance: {balance:.2f} USDT")
        except Exception as e:
            logger.error(f"Error connecting to account: {e}")
            raise    

    def _load_trading_history(self):
        """Load trading history from file if exists"""
        try:
            with open('trading_history.json', 'r') as f:
                history = json.load(f)
                self.stats.trade_history = history
                logger.info(f"Loaded {len(history)} historical trades")
        except FileNotFoundError:
            logger.info("No trading history file found")

    def _save_trading_history(self):
        """Save trading history to file"""
        try:
            with open('trading_history.json', 'w') as f:
                json.dump(self.stats.trade_history, f)
        except Exception as e:
            logger.error(f"Error saving trading history: {e}")

    def _init_telegram(self):
        """Initialize Telegram application with connection test"""
        try:
            # Store the event loop as an instance variable
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.telegram_app = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()
            
            # Initialize with a longer timeout for first connection
            async def init_and_test():
                await self.telegram_app.initialize()
                # Test connection with a simple get_me call
                await self.telegram_app.bot.get_me()
            
            self.loop.run_until_complete(asyncio.wait_for(init_and_test(), timeout=30))
            print("✓ Telegram bot initialized and connected successfully")
        except Exception as e:
            print(f"Warning: Telegram initialization error: {e}")
            print("Bot will continue running, but Telegram notifications may be delayed")
            self.telegram_app = None
            if hasattr(self, 'loop') and self.loop.is_running():
                self.loop.close()

    async def send_telegram(self, message):
        """Send message to Telegram channel with timeout handling"""
        if self.telegram_app is None:
            print("Telegram bot not initialized")
            return
        
        try:
            await asyncio.wait_for(
                self.telegram_app.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    text=message,
                    parse_mode=ParseMode.HTML
                ),
                timeout=10  # 10 seconds timeout
            )
        except asyncio.TimeoutError:
            print(f"Telegram message timed out, retrying once: {message[:20]}...")
            try:
                await asyncio.wait_for(
                    self.telegram_app.bot.send_message(
                        chat_id=Config.TELEGRAM_CHAT_ID,
                        text=message,
                        parse_mode=ParseMode.HTML
                    ),
                    timeout=20  # 20 seconds timeout
                )
            except Exception as e:
                print(f"Retry failed: {e}")
        except Exception as e:
            print(f"Error sending Telegram message: {e}")

    def send_telegram_message(self, message):
        """Synchronous wrapper for send_telegram with improved error handling"""
        try:
            # Use the existing event loop
            if not hasattr(self, 'loop') or self.loop.is_closed():
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            
            self.loop.run_until_complete(self.send_telegram(message))
        except Exception as e:
            print(f"Error in send_telegram_message: {e}")
            # Store failed message for later retry
            if not hasattr(self, '_failed_messages'):
                self._failed_messages = []
            self._failed_messages.append(message)

    def get_market_data(self, symbol: str) -> Optional[Dict]:
        """Get comprehensive market data for a symbol"""
        try:
            ticker = self.client.futures_ticker(symbol=symbol)
            depth = self.client.futures_order_book(symbol=symbol)
            
            spread = (float(depth['asks'][0][0]) - float(depth['bids'][0][0])) / float(depth['bids'][0][0]) * 100
            
            return {
                'price': float(ticker['lastPrice']),
                'volume_24h': float(ticker['volume']),
                'price_change_24h': float(ticker['priceChangePercent']),
                'spread': spread
            }
        except Exception as e:
            logger.error(f"Error getting market data for {symbol}: {e}")
            return None

    def check_trade_conditions(self, symbol: str, side: str) -> bool:
        """Check if market conditions are suitable for trading"""
        market_data = self.get_market_data(symbol)
        if not market_data:
            return False
            
        # Check market conditions
        if market_data['volume_24h'] < Config.MIN_VOLUME_24H:
            logger.warning(f"{symbol} volume too low: {market_data['volume_24h']:.2f} USDT")
            return False
            
        if market_data['spread'] > Config.MAX_SPREAD_PERCENT:
            logger.warning(f"{symbol} spread too high: {market_data['spread']:.3f}%")
            return False
            
        return True

    def get_position_size(self, symbol: str) -> Optional[float]:
        """Calculate position size with proper precision handling"""
        try:
            symbol_info = self.symbol_data[symbol]
            
            # Get account balance
            account = self.client.futures_account()
            balance = float(account['totalWalletBalance'])
            
            # Get current price
            price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
            if price <= 0:
                logger.error(f"Invalid price for {symbol}: {price}")
                return None
                
            # Calculate position size
            position_value = balance * Config.TRADE_AMOUNT
            position_size = (position_value * Config.MAX_LEVERAGE) / price
            
            # Apply precision
            precision = symbol_info['precision']['quantity']
            position_size = round(position_size, precision)
            
            # Apply minimum quantity
            lot_size = symbol_info['filters'].get('LOT_SIZE', {})
            min_qty = float(lot_size.get('minQty', 0))
            if position_size < min_qty:
                position_size = min_qty
            
            logger.info(f"Calculated position size for {symbol}: {position_size}")
            return position_size
            
        except Exception as e:
            logger.error(f"Error calculating position size for {symbol}: {e}")
            return None

    def place_trade(self, symbol: str, side: str, quantity: float) -> bool:
        """Place trade with enhanced validation and risk management"""
        try:
            if not self.check_trade_conditions(symbol, side):
                return False
                
            logger.info(f"Placing {side} trade for {symbol}")
            
            # Create order with proper error handling
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=quantity
            )
            
            # Get entry price
            fills = order.get('fills', [])
            if fills:
                entry_price = sum(float(fill['price']) * float(fill['qty']) for fill in fills) / sum(float(fill['qty']) for fill in fills)
            else:
                entry_price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
            
            # Store trade information
            self.open_trades[symbol] = {
                'side': side,
                'entry_price': entry_price,
                'quantity': quantity,
                'entry_time': datetime.now(),
                'order_id': order['orderId'],
                'highest_price': entry_price,
                'lowest_price': entry_price
            }
            
            # Send notification
            message = (
                f"🔔 New {side} Trade\n"
                f"Symbol: {symbol}\n"
                f"Quantity: {quantity}\n"
                f"Entry: {entry_price:.4f}\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            self.loop.run_until_complete(self.send_telegram(message))
            
            return True
            
        except BinanceAPIException as e:
            logger.error(f"Binance API Error: {e.message}")
            return False
        except Exception as e:
            logger.error(f"Unexpected trade error: {e}")
            return False

    def calculate_trailing_stop(self, symbol: str, trade: dict, current_price: float) -> float:
        """Calculate dynamic trailing stop based on price action and volatility"""
        try:
            # Get ATR for volatility measurement
            klines = self.client.futures_klines(symbol=symbol, interval='1h', limit=self.Config.ATR_PERIOD + 1)
            highs = np.array([float(x[2]) for x in klines])
            lows = np.array([float(x[3]) for x in klines])
            closes = np.array([float(x[4]) for x in klines])
            atr = talib.ATR(highs, lows, closes, timeperiod=self.Config.ATR_PERIOD)[-1]
            
            entry_price = trade['entry_price']
            
            if trade['side'] == 'BUY':
                # Calculate current profit percentage
                current_profit = ((current_price - entry_price) / entry_price) * 100
                
                # Start with initial wide trailing stop
                if current_profit < self.Config.TRAILING_ACTIVATION:
                    trailing_percentage = self.Config.INITIAL_TRAILING_STOP
                else:
                    # Tighten stop as profit increases
                    profit_steps = (current_profit - self.Config.TRAILING_ACTIVATION) // self.Config.TIGHTENING_STEP
                    trailing_percentage = max(
                        self.Config.INITIAL_TRAILING_STOP - (profit_steps * 0.1),
                        self.Config.MIN_TRAILING_STOP
                    )
                
                # Adjust based on volatility (ATR)
                atr_percentage = (atr / current_price) * 100
                trailing_percentage = max(trailing_percentage, atr_percentage * 0.5)
                
                # Calculate stop price
                trailing_stop_price = max(
                    trade['highest_price'] * (1 - trailing_percentage/100),
                    entry_price * (1 + self.Config.MIN_TRAILING_STOP/100)  # Never go below small profit
                )
                
            else:  # SELL/SHORT position
                current_profit = ((entry_price - current_price) / entry_price) * 100
                
                if current_profit < self.Config.TRAILING_ACTIVATION:
                    trailing_percentage = self.Config.INITIAL_TRAILING_STOP
                else:
                    profit_steps = (current_profit - self.Config.TRAILING_ACTIVATION) // self.Config.TIGHTENING_STEP
                    trailing_percentage = max(
                        self.Config.INITIAL_TRAILING_STOP - (profit_steps * 0.1),
                        self.Config.MIN_TRAILING_STOP
                    )
                
                atr_percentage = (atr / current_price) * 100
                trailing_percentage = max(trailing_percentage, atr_percentage * 0.5)
                
                trailing_stop_price = min(
                    trade['lowest_price'] * (1 + trailing_percentage/100),
                    entry_price * (1 - self.Config.MIN_TRAILING_STOP/100)
                )
                
            return trailing_stop_price
            
        except Exception as e:
            logger.error(f"Error calculating dynamic trailing stop: {e}")
            # Fallback to basic trailing stop
            if trade['side'] == 'BUY':
                return trade['highest_price'] * (1 - self.Config.INITIAL_TRAILING_STOP/100)
            else:
                return trade['lowest_price'] * (1 + self.Config.INITIAL_TRAILING_STOP/100)            
                

    def get_indicators(self, symbol: str) -> Dict[str, Dict]:
        """Calculate RSI Divergence, MACD, and EMAs"""
        indicators = {}
        
        try:
            for timeframe in Config.TIMEFRAMES:
                klines = self.client.futures_klines(symbol=symbol, interval=timeframe, limit=100)
                
                if not klines:
                    logger.error(f"No klines data received for {symbol} on {timeframe}")
                    continue
                    
                closes = np.array([float(x[4]) for x in klines])
                
                # Calculate RSI and check divergence
                rsi = talib.RSI(closes, timeperiod=Config.RSI_PERIOD)
                
                # Calculate MACD
                macd, signal, hist = talib.MACD(closes, 
                                              fastperiod=Config.MACD_FAST,
                                              slowperiod=Config.MACD_SLOW,
                                              signalperiod=Config.MACD_SIGNAL)
                
                # Calculate EMAs
                ema20 = talib.EMA(closes, timeperiod=20)
                ema50 = talib.EMA(closes, timeperiod=50)
                
                indicators[timeframe] = {
                    'rsi': {
                        'current': rsi[-1],
                        'prev': rsi[-2],
                        'divergence': self._check_rsi_divergence(rsi, closes)
                    },
                    'macd': {
                        'macd': macd[-1],
                        'macd_prev': macd[-2],
                        'signal': signal[-1],
                        'signal_prev': signal[-2],
                        'hist': hist[-1],
                        'hist_prev': hist[-2]
                    },
                    'trend': {
                        'ema20': ema20[-1],
                        'ema50': ema50[-1]
                    }
                }
                
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol}: {e}")
            return {}

    def _check_rsi_divergence(self, rsi: np.array, price: np.array, periods: int = 10) -> str:
        """Check for RSI divergence"""
        price_window = price[-periods:]
        rsi_window = rsi[-periods:]
        
        price_min_idx = np.argmin(price_window)
        price_max_idx = np.argmax(price_window)
        rsi_min_idx = np.argmin(rsi_window)
        rsi_max_idx = np.argmax(rsi_window)
        
        # Bullish divergence: price makes lower low but RSI makes higher low
        if price_min_idx > rsi_min_idx:
            return "bullish"
        # Bearish divergence: price makes higher high but RSI makes lower high
        elif price_max_idx > rsi_max_idx:
            return "bearish"
        return "none"

    def analyze_trend(self, trend_indicators: Dict) -> Tuple[str, float]:
        """Analyze trend strength and direction using MACD and EMA"""
        try:
            macd = trend_indicators['macd']
            hist = trend_indicators['hist']
            ema_trend = trend_indicators['ema_trend']
            
            # Determine trend direction and strength
            if macd > Config.MACD_THRESHOLD and ema_trend > Config.EMA_THRESHOLD:
                trend = "STRONG_BULLISH" if hist > 0 else "BULLISH"
                strength = abs(ema_trend)
            elif macd < -Config.MACD_THRESHOLD and ema_trend < -Config.EMA_THRESHOLD:
                trend = "STRONG_BEARISH" if hist < 0 else "BEARISH"
                strength = abs(ema_trend)
            else:
                trend = "NEUTRAL"
                strength = 0
                
            return trend, strength
            
        except Exception as e:
            logger.error(f"Error analyzing trend: {e}")
            return "NEUTRAL", 0
            

    def manage_trades(self):
        """Monitor and manage open trades with enhanced risk management"""
        for symbol in list(self.open_trades.keys()):
            try:
                trade = self.open_trades[symbol]
                current_price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
                
                # Skip if invalid price
                if current_price <= 0:
                    logger.error(f"Invalid price for {symbol}: {current_price}")
                    continue
                
                # Update trade metrics
                if trade['side'] == 'BUY':
                    trade['highest_price'] = max(trade['highest_price'], current_price)
                    trailing_stop_price = trade['highest_price'] * (1 - Config.INITIAL_TRAILING_STOP/100)
                    
                    # Check exit conditions
                    should_exit = (
                        current_price <= trailing_stop_price or
                        current_price <= trade['entry_price'] * (1 - Config.MAX_STOP_LOSS/100) or
                        current_price >= trade['entry_price'] * (1 + Config.MAX_PROFIT/100)
                    )
                    
                    if should_exit:
                        self.close_trade(symbol, 'SELL')
                
                else:  # SHORT trade
                    trade['lowest_price'] = min(trade['lowest_price'], current_price)
                    trailing_stop_price = trade['lowest_price'] * (1 + Config.INITIAL_TRAILING_STOP/100)
                    
                    # Check exit conditions
                    should_exit = (
                        current_price >= trailing_stop_price or
                        current_price >= trade['entry_price'] * (1 + Config.MAX_STOP_LOSS/100) or
                        current_price <= trade['entry_price'] * (1 - Config.MAX_PROFIT/100)
                    )
                    
                    if should_exit:
                        self.close_trade(symbol, 'BUY')
                        
            except Exception as e:
                logger.error(f"Error managing trade for {symbol}: {e}")

    def check_signal(self, timeframe_data: Dict) -> Optional[str]:
        """Check for valid trading signals using RSI Divergence, MACD, and EMAs"""
        rsi = timeframe_data['rsi']
        macd = timeframe_data['macd']
        trend = timeframe_data['trend']
        
        # Check for long signal
        long_signal = (
            rsi['divergence'] == "bullish" and  # RSI shows bullish divergence
            macd['macd'] > macd['signal'] and  # MACD above signal line
            macd['hist'] > macd['hist_prev'] and  # MACD histogram increasing
            trend['ema20'] > trend['ema50']  # Uptrend confirmation
        )
        
        # Check for short signal
        short_signal = (
            rsi['divergence'] == "bearish" and  # RSI shows bearish divergence
            macd['macd'] < macd['signal'] and  # MACD below signal line
            macd['hist'] < macd['hist_prev'] and  # MACD histogram decreasing
            trend['ema20'] < trend['ema50']  # Downtrend confirmation
        )
        
        if long_signal:
            return "LONG"
        elif short_signal:
            return "SHORT"
        
        return None


    def close_trade(self, symbol: str, side: str):
        """Close trade with comprehensive reporting"""
        try:
            trade = self.open_trades[symbol]
            
            # Close position
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=trade['quantity']
            )
            
            # Calculate exit price and PnL
            fills = order.get('fills', [])
            if fills:
                exit_price = sum(float(fill['price']) * float(fill['qty']) for fill in fills) / sum(float(fill['qty']) for fill in fills)
            else:
                exit_price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
            
            entry_price = trade['entry_price']
            pnl = ((exit_price - entry_price) / entry_price * 100) * (-1 if side == 'BUY' else 1)
            
            # Record trade data
            trade_data = {
                'symbol': symbol,
                'side': trade['side'],
                'entry_price': entry_price,
                'exit_price': exit_price,
                'quantity': trade['quantity'],
                'pnl': pnl,
                'entry_time': trade['entry_time'].isoformat(),
                'exit_time': datetime.now().isoformat(),
                'duration': str(datetime.now() - trade['entry_time'])
            }
            
            self.stats.add_trade(trade_data)
            self._save_trading_history()
            
            # Send notification
            message = (
                f"🔔 Trade Closed\n"
                f"Symbol: {symbol}\n"
                f"Side: {trade['side']}\n"
                f"P/L: {pnl:.2f}%\n"
                f"Entry: {entry_price:.4f}\n"
                f"Exit: {exit_price:.4f}\n"
                f"Duration: {trade_data['duration']}"
            )
            self.loop.run_until_complete(self.send_telegram(message))
            
            del self.open_trades[symbol]
            
        except Exception as e:
            logger.error(f"Error closing trade for {symbol}: {e}")

    def check_signals(self):
        """Check for trading signals across all timeframes"""
        if len(self.open_trades) >= Config.MAX_OPEN_TRADES:
            return
            
        status_data = []
        
        for symbol in Config.SYMBOLS:
            try:
                if symbol in self.open_trades:
                    continue
                    
                market_data = self.get_market_data(symbol)
                if not market_data:
                    continue
                    
                timeframe_indicators = self.get_indicators(symbol)
                if not timeframe_indicators:
                    continue
                
                current_price = market_data['price']
                
                # Check signals for each timeframe
                signals = []
                for timeframe, indicators in timeframe_indicators.items():
                    signal = self.check_signal(indicators)
                    if signal:
                        signals.append((signal, timeframe))
                    
                    # Record status
                    status_data.append([
                        symbol,
                        timeframe,
                        f"{indicators['rsi']['current']:.2f}",
                        f"{indicators['rsi']['divergence']}",
                        f"{indicators['macd']['hist']:.4f}",
                        "Yes" if indicators['macd']['macd'] > indicators['macd']['signal'] else "No",
                        signal if signal else "None",
                        f"{current_price:.4f}"
                    ])
                
                # Execute trade if we have signals and conditions are met
                if signals and self.check_trade_conditions(symbol, signals[0][0]):
                    quantity = self.get_position_size(symbol)
                    if quantity:
                        signal_str = ", ".join([f"{s[0]} ({s[1]})" for s in signals])
                        logger.info(f"Signals detected for {symbol}: {signal_str}")
                        self.place_trade(symbol, 'BUY' if signals[0][0] == 'LONG' else 'SELL', quantity)
                        
            except Exception as e:
                logger.error(f"Error checking signals for {symbol}: {e}")
                
        # Display status table
        if status_data:
            print("\nMarket Analysis:")
            print(tabulate(
                status_data,
                headers=['Symbol', 'TF', 'RSI', 'RSI Div', 'MACD Hist', 'MACD Cross', 'Signal', 'Price'],
                tablefmt='grid'
            ))

        
    def display_open_trades(self):
        """Display current open trades with enhanced metrics"""
        if not self.open_trades:
            return
            
        trades_data = []
        total_pnl = 0
        
        for symbol, trade in self.open_trades.items():
            try:
                current_price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
                pnl = ((current_price - trade['entry_price']) / trade['entry_price'] * 100) * \
                      (1 if trade['side'] == 'BUY' else -1)
                      
                duration = datetime.now() - trade['entry_time']
                total_pnl += pnl
                
                trades_data.append([
                    symbol,
                    trade['side'],
                    f"{trade['entry_price']:.4f}",
                    f"{current_price:.4f}",
                    f"{pnl:.2f}%",
                    str(duration).split('.')[0],
                    f"{trade['quantity']}"
                ])
                
            except Exception as e:
                logger.error(f"Error displaying trade for {symbol}: {e}")
                
        if trades_data:
            print("\nOpen Trades:")
            print(tabulate(
                trades_data,
                headers=['Symbol', 'Side', 'Entry', 'Current', 'P/L', 'Duration', 'Size'],
                tablefmt='grid'
            ))
            print(f"Total P/L: {total_pnl:.2f}%")


    def run(self):
        """Main bot loop with enhanced error handling and statistics"""
        self.running = True
        error_count = 0
        max_consecutive_errors = 5
        last_status_update = datetime.now()
        status_update_interval = timedelta(hours=1)
        
        logger.info("Bot starting...")
        self.loop.run_until_complete(self.send_telegram("🟢 Trading Bot Started"))
        
        try:
            while self.running:
                try:
                    # Main trading operations
                    self.check_signals()
                    self.manage_trades()
                    self.display_open_trades()
                    
                    # Periodic status update
                    if datetime.now() - last_status_update > status_update_interval:
                        win_rate = self.stats.get_win_rate()
                        status_message = (
                            f"📊 Bot Status Update\n"
                            f"Total Trades: {self.stats.total_trades}\n"
                            f"Win Rate: {win_rate:.1%}\n"
                            f"Open Trades: {len(self.open_trades)}"
                        )
                        self.loop.run_until_complete(self.send_telegram(status_message))
                        last_status_update = datetime.now()
                    
                    error_count = 0
                    time.sleep(1)
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    
                    if error_count >= max_consecutive_errors:
                        logger.critical(f"Stopping bot due to {error_count} consecutive errors")
                        self.loop.run_until_complete(self.send_telegram(
                            f"🛑 Bot stopped after {error_count} consecutive errors"
                        ))
                        break
                        
                    time.sleep(10)
                    
        except KeyboardInterrupt:
            logger.info("Bot manually stopped")
            self.loop.run_until_complete(self.send_telegram("🔴 Bot Manually Stopped"))
            
        finally:
            self._save_trading_history()

if __name__ == "__main__":
    try:
        logger = setup_logging()
        bot = TradingBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Bot manually stopped")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)