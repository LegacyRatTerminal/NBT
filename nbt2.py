from time import time, sleep
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
import sys
import json
from typing import Dict, List, Optional, Tuple
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from requests.adapters import HTTPAdapter, Retry
from urllib3.util.retry import Retry

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
    BINANCE_API_KEY = 'rbKr65FG6zXB7ACrhIlDmbkXHOqIKCVCMAuTPVmEOMvfxjD4i1JlpTVnEhakKK9I'
    BINANCE_API_SECRET = 'gjIYYMJ0DB5KtNfRq2WxdUoxgi53uOdPoRuyCcSuX1vBqhA0SpMOO0L75WwfNQio'
    TELEGRAM_BOT_TOKEN = '7892769491:AAEVZuT2pQmL6034tfUt8EiV1DLDDX-XxCw'
    TELEGRAM_CHAT_ID = '7917402606'

    # Trading parameters
    SYMBOLS = ['BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'LINKUSDT', 'AAVEUSDT', 'ADAUSDT', 'EOSUSDT', 'ETCUSDT', 'UNIUSDT', 'ORDIUSDT', 'AGLDUSDT']
    TIMEFRAMES = {  
        '1m': '5m',
        '5m': '15m',
        '15m': '30m',
        '30m': '1h'
    }

    # Indicator parameters
    RSI_PERIOD = 14
    RSI_OVERSOLD = 28  # Relaxed from 35
    RSI_OVERBOUGHT = 72  # Relaxed from 65
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    
    # Risk management
    TRADE_AMOUNT = 0.4  # 40% of balance
    MAX_OPEN_TRADES = 2
    MAX_STOP_LOSS = 0.8 # 1%
    MAX_PROFIT = 0.1  # 2%
    MAX_LEVERAGE = 20

    # Enhanced trailing stop settings
    INITIAL_TRAILING_STOP = 0.8  # Initial trailing stop percentage (wider)
    MIN_TRAILING_STOP = 0.4      # Minimum trailing stop as price moves in favor
    TRAILING_ACTIVATION = 0.8    # % profit needed before tightening trailing stop
    TIGHTENING_STEP = 0.2       # How much to tighten trail stop as profit increases
    
    # Market conditions
    MIN_VOLUME_24H = 400000  # Minimum 24h volume in USDT
    MAX_SPREAD_PERCENT = 0.2  # Maximum allowed spread (0.2%)
    
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


class DataCache:
    def __init__(self, ttl=5):  # TTL in seconds
        self.cache = {}
        self.ttl = ttl
        
    def get(self, key):
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time() - timestamp < self.ttl:
                return data
            del self.cache[key]
        return None
        
    def set(self, key, value):
        self.cache[key] = (value, time())       


class TradingBot:
    def __init__(self):
        """Initialize the TradingBot with enhanced features"""
        try:
            self._validate_config()
            self.client = Client(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET)
            self.client.session.mount('https://', HTTPAdapter(
                pool_connections=25,
                pool_maxsize=25,
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.1,
                    status_forcelist=[429, 500, 502, 503, 504]
                )
            ))
            self._init_telegram()
            
            self.open_trades: Dict = {}
            self.running = False
            self.stats = TradingStats()
            self.symbol_data: Dict = {}
            
            # Add caches
            self.indicator_cache = DataCache(ttl=5)  # Cache indicators for 1 minute
            self.market_data_cache = DataCache(ttl=5)  # Cache market data for 5 seconds
            
            self._setup_trading_environment()
            self._load_trading_history()
            
            # Initialize thread pool
            self.thread_pool = ThreadPoolExecutor(max_workers=len(Config.SYMBOLS))
            
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

    def get_market_data_batch(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get market data for multiple symbols in one batch"""
        try:
            # Check cache first
            cached_data = {}
            symbols_to_fetch = []
            
            for symbol in symbols:
                cached = self.market_data_cache.get(symbol)
                if cached:
                    cached_data[symbol] = cached
                else:
                    symbols_to_fetch.append(symbol)
                    
            if not symbols_to_fetch:
                return cached_data
                
            # Batch ticker request
            tickers = self.client.futures_ticker()
            ticker_data = {t['symbol']: t for t in tickers}
            
            # Get order book data for each symbol individually
            depth_data = {}
            for symbol in symbols_to_fetch:
                try:
                    depth = self.client.futures_order_book(
                        symbol=symbol,
                        limit=5
                    )
                    depth_data[symbol] = depth
                except Exception as e:
                    logger.error(f"Error getting order book for {symbol}: {e}")
                    continue
            
            market_data = cached_data.copy()
            for symbol in symbols_to_fetch:
                if symbol in ticker_data and symbol in depth_data:
                    ticker = ticker_data[symbol]
                    depth = depth_data[symbol]
                    
                    data = {
                        'price': float(ticker['lastPrice']),
                        'volume_24h': float(ticker['volume']),
                        'price_change_24h': float(ticker['priceChangePercent']),
                        'spread': (float(depth['asks'][0][0]) - float(depth['bids'][0][0])) / \
                                 float(depth['bids'][0][0]) * 100
                    }
                    
                    market_data[symbol] = data
                    self.market_data_cache.set(symbol, data)
                    
            return market_data
            
        except Exception as e:
            logger.error(f"Error in batch market data: {e}")
            return {}   

    def get_market_data(self, symbol: str) -> Optional[Dict]:
        """
        Get market data for a single symbol, with caching
        Returns a dictionary with price, volume, and spread information
        """
        try:
            # Check cache first
            cached_data = self.market_data_cache.get(symbol)
            if cached_data:
                return cached_data
                
            # Get fresh data using batch method for efficiency
            market_data = self.get_market_data_batch([symbol])
            if symbol in market_data:
                self.market_data_cache.set(symbol, market_data[symbol])
                return market_data[symbol]
                
            return None
            
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

    def get_current_position(self, symbol: str) -> Optional[Dict]:
        """Get current position details for a symbol"""
        try:
            position = self.client.futures_position_information(symbol=symbol)[0]
            qty = float(position['positionAmt'])
            if qty != 0:
                return {
                    'quantity': abs(qty),
                    'side': 'BUY' if qty > 0 else 'SELL',
                    'entry_price': float(position['entryPrice'])
                }
            return None
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return None                

    def place_trade(self, symbol: str, side: str, quantity: float) -> bool:
        """Place trade with position checking"""
        try:
            # Check existing position
            current_position = self.get_current_position(symbol)
            if current_position:
                logger.warning(f"Position already exists for {symbol}. Skipping trade.")
                return False
                
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
            
            # Get entry price from fills
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
        """
        Calculate dynamic trailing stop with proper price tracking and risk management.
        Returns the calculated stop price.
        """
        try:
            entry_price = trade['entry_price']
            is_long = trade['side'] == 'BUY'
            
            # Calculate current profit percentage (unified for both directions)
            profit_multiplier = 1 if is_long else -1
            current_profit = ((current_price - entry_price) / entry_price) * 100 * profit_multiplier
            
            # Update high/low water marks
            if is_long:
                trade['highest_price'] = max(trade['highest_price'], current_price)
                reference_price = trade['highest_price']
            else:
                trade['lowest_price'] = min(trade['lowest_price'], current_price)
                reference_price = trade['lowest_price']
                
            # Determine trailing stop percentage based on profit level
            if current_profit < Config.TRAILING_ACTIVATION:
                trailing_percentage = Config.INITIAL_TRAILING_STOP
            else:
                # Progressive tightening as profit increases
                profit_steps = (current_profit - Config.TRAILING_ACTIVATION) // Config.TIGHTENING_STEP
                trailing_percentage = max(
                    Config.INITIAL_TRAILING_STOP - (profit_steps * Config.TIGHTENING_STEP),
                    Config.MIN_TRAILING_STOP
                )
                
            # Calculate stop price based on position direction
            if is_long:
                stop_price = reference_price * (1 - trailing_percentage/100)
            else:
                stop_price = reference_price * (1 + trailing_percentage/100)
                
            return stop_price
            
        except Exception as e:
            logger.error(f"Error in calculate_trailing_stop for {symbol}: {e}")
            # Return a conservative fallback based on the last known reference price
            if 'highest_price' in trade and is_long:
                return trade['highest_price'] * (1 - Config.INITIAL_TRAILING_STOP/100)
            elif 'lowest_price' in trade and not is_long:
                return trade['lowest_price'] * (1 + Config.INITIAL_TRAILING_STOP/100)
            else:
                # Ultimate fallback using entry price if no reference price available
                return entry_price * (1 + (Config.INITIAL_TRAILING_STOP/100 * (-1 if is_long else 1)))             


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
                
                # Calculate trailing stop using the improved function
                trailing_stop_price = self.calculate_trailing_stop(symbol, trade, current_price)
                is_long = trade['side'] == 'BUY'
                
                # Determine exit conditions based on position direction
                should_exit = False
                
                if is_long:
                    # Long position exit conditions
                    stop_hit = current_price <= trailing_stop_price
                    max_loss_hit = current_price <= trade['entry_price'] * (1 - Config.MAX_STOP_LOSS/100)
                    take_profit_hit = current_price >= trade['entry_price'] * (1 + Config.MAX_PROFIT/100)
                    
                    should_exit = stop_hit or max_loss_hit or take_profit_hit
                    exit_reason = (
                        "Trailing Stop" if stop_hit else
                        "Stop Loss" if max_loss_hit else
                        "Take Profit" if take_profit_hit else
                        "Unknown"
                    )
                    
                else:
                    # Short position exit conditions
                    stop_hit = current_price >= trailing_stop_price
                    max_loss_hit = current_price >= trade['entry_price'] * (1 + Config.MAX_STOP_LOSS/100)
                    take_profit_hit = current_price <= trade['entry_price'] * (1 - Config.MAX_PROFIT/100)
                    
                    should_exit = stop_hit or max_loss_hit or take_profit_hit
                    exit_reason = (
                        "Trailing Stop" if stop_hit else
                        "Stop Loss" if max_loss_hit else
                        "Take Profit" if take_profit_hit else
                        "Unknown"
                    )
                
                if should_exit:
                    logger.info(f"Exiting {symbol} trade due to {exit_reason}")
                    self.close_trade(symbol, 'SELL' if is_long else 'BUY')
                    
            except Exception as e:
                logger.error(f"Error managing trade for {symbol}: {e}")



    def close_trade(self, symbol: str, side: str):
        """Close entire position for a symbol"""
        try:
            # Get current position
            current_position = self.get_current_position(symbol)
            if not current_position:
                logger.warning(f"No position found for {symbol}")
                if symbol in self.open_trades:
                    del self.open_trades[symbol]
                return
            
            # Close entire position
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=current_position['quantity'],
                reduceOnly=True  # Ensure we only close existing position
            )
            
            # Calculate exit price and PnL
            fills = order.get('fills', [])
            if fills:
                exit_price = sum(float(fill['price']) * float(fill['qty']) for fill in fills) / sum(float(fill['qty']) for fill in fills)
            else:
                exit_price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
            
            trade = self.open_trades.get(symbol, {})
            entry_price = trade.get('entry_price', current_position['entry_price'])
            pnl = ((exit_price - entry_price) / entry_price * 100) * (-1 if side == 'BUY' else 1)
            
            # Record trade data
            trade_data = {
                'symbol': symbol,
                'side': current_position['side'],
                'entry_price': entry_price,
                'exit_price': exit_price,
                'quantity': current_position['quantity'],
                'pnl': pnl,
                'entry_time': trade.get('entry_time', datetime.now() - timedelta(minutes=1)).isoformat(),
                'exit_time': datetime.now().isoformat(),
                'duration': str(datetime.now() - trade.get('entry_time', datetime.now() - timedelta(minutes=1)))
            }
            
            self.stats.add_trade(trade_data)
            self._save_trading_history()
            
            message = (
                f"🔔 Trade Closed\n"
                f"Symbol: {symbol}\n"
                f"Side: {current_position['side']}\n"
                f"P/L: {pnl:.2f}%\n"
                f"Entry: {entry_price:.4f}\n"
                f"Exit: {exit_price:.4f}\n"
                f"Duration: {trade_data['duration']}"
            )
            self.loop.run_until_complete(self.send_telegram(message))
            
            if symbol in self.open_trades:
                del self.open_trades[symbol]
                
        except Exception as e:
            logger.error(f"Error closing trade for {symbol}: {e}")


    def get_multi_timeframe_signals(self, symbol: str) -> Dict[str, Dict]:
        """
        Analyze trading signals across multiple timeframes with relaxed confirmation
        Returns a dictionary of valid signals with their strength and timeframe info
        """
        try:
            # Get indicators for all timeframes
            timeframe_indicators = self.get_indicators(symbol)
            if not timeframe_indicators:
                return {}
                
            valid_signals = {}
            
            # Analyze primary timeframes with confirmation from higher timeframes
            for primary_tf, confirm_tf in Config.TIMEFRAMES.items():
                # Skip if we don't have data for either timeframe
                if primary_tf not in timeframe_indicators or confirm_tf not in timeframe_indicators:
                    continue
                    
                primary_data = timeframe_indicators[primary_tf]
                confirm_data = timeframe_indicators[confirm_tf]
                
                # For long signals - Relaxed conditions
                long_conditions = {
                    'primary': (
                        primary_data['rsi']['current'] < Config.RSI_OVERSOLD and
                        primary_data['macd']['hist'] > primary_data['macd']['hist_prev'] and
                        # Removed positive MACD histogram requirement
                        abs(primary_data['macd']['hist'] - primary_data['macd']['hist_prev']) > 0.0006  # Reduced momentum threshold
                    ),
                    'confirm': (
                        confirm_data['rsi']['current'] < 50 and  # Relaxed RSI requirement
                        (confirm_data['trend']['ema20'] > confirm_data['trend']['ema50'] or  # Either EMA trend
                         confirm_data['macd']['hist'] > confirm_data['macd']['hist_prev'])   # or MACD trend is positive
                    )
                }

                # For short signals - Relaxed conditions
                short_conditions = {
                    'primary': (
                        primary_data['rsi']['current'] > Config.RSI_OVERBOUGHT and
                        primary_data['macd']['hist'] < primary_data['macd']['hist_prev'] and
                        # Removed negative MACD histogram requirement
                        abs(primary_data['macd']['hist'] - primary_data['macd']['hist_prev']) > 0.0006  # Reduced momentum threshold
                    ),
                    'confirm': (
                        confirm_data['rsi']['current'] > 50 and  # Relaxed RSI requirement
                        (confirm_data['trend']['ema20'] < confirm_data['trend']['ema50'] or  # Either EMA trend
                         confirm_data['macd']['hist'] < confirm_data['macd']['hist_prev'])   # or MACD trend is negative
                    )
                }
                
                # Calculate signal strength (0-1) with modified weights
                if long_conditions['primary'] and long_conditions['confirm']:
                    rsi_strength = (Config.RSI_OVERSOLD - primary_data['rsi']['current']) / Config.RSI_OVERSOLD
                    macd_strength = abs(primary_data['macd']['hist']) / (abs(primary_data['macd']['hist_prev']) + 0.00001)
                    trend_strength = 1 if primary_data['trend']['ema20'] > primary_data['trend']['ema50'] else 0.5
                    
                    strength = (rsi_strength * 0.4 + macd_strength * 0.4 + trend_strength * 0.2)
                    
                    valid_signals[f"{primary_tf}_LONG"] = {
                        'signal': 'LONG',
                        'strength': strength,
                        'primary_tf': primary_tf,
                        'confirm_tf': confirm_tf,
                        'rsi': primary_data['rsi']['current'],
                        'macd_hist': primary_data['macd']['hist']
                    }
                    
                elif short_conditions['primary'] and short_conditions['confirm']:
                    rsi_strength = (primary_data['rsi']['current'] - Config.RSI_OVERBOUGHT) / (100 - Config.RSI_OVERBOUGHT)
                    macd_strength = abs(primary_data['macd']['hist']) / (abs(primary_data['macd']['hist_prev']) + 0.00001)
                    trend_strength = 1 if primary_data['trend']['ema20'] < primary_data['trend']['ema50'] else 0.5
                    
                    strength = (rsi_strength * 0.4 + macd_strength * 0.4 + trend_strength * 0.2)
                    
                    valid_signals[f"{primary_tf}_SHORT"] = {
                        'signal': 'SHORT',
                        'strength': strength,
                        'primary_tf': primary_tf,
                        'confirm_tf': confirm_tf,
                        'rsi': primary_data['rsi']['current'],
                        'macd_hist': primary_data['macd']['hist']
                    }
            
            return valid_signals
            
        except Exception as e:
            logger.error(f"Error analyzing multi-timeframe signals for {symbol}: {e}")
            return {}        
                
                            
    def check_signals(self):
        """Check for trading signals across all timeframes with parallel processing"""
        status_data = []
        
        def process_symbol(symbol: str) -> Optional[Dict]:
            try:
                # Get cached market data
                market_data = self.market_data_cache.get(symbol)
                if not market_data:
                    return None
                    
                current_price = market_data['price']
                
                # Get indicators with caching
                cached_indicators = self.indicator_cache.get(f"{symbol}_indicators")
                if cached_indicators:
                    timeframe_indicators = cached_indicators
                else:
                    timeframe_indicators = self.get_indicators(symbol)
                    if timeframe_indicators:
                        self.indicator_cache.set(f"{symbol}_indicators", timeframe_indicators)
                
                if not timeframe_indicators:
                    return None
                
                # Process signals
                try:
                    timeframe_signals = self.get_multi_timeframe_signals(symbol)
                except Exception as e:
                    logger.error(f"Error getting signals for {symbol}: {e}")
                    timeframe_signals = {}
                
                symbol_data = []
                
                # Process each timeframe
                for tf in Config.TIMEFRAMES:
                    if tf in timeframe_indicators:
                        tf_data = timeframe_indicators[tf]
                        
                        rsi_current = tf_data.get('rsi', {}).get('current', 0)
                        rsi_div = tf_data.get('rsi', {}).get('divergence', 'none')
                        
                        macd_data = tf_data.get('macd', {})
                        macd_hist = macd_data.get('hist', 0)
                        macd_cross = "Yes" if macd_data.get('macd', 0) > macd_data.get('signal', 0) else "No"
                        
                        signal = (timeframe_signals.get(f"{tf}_LONG", {}).get('signal', 'None') or 
                                 timeframe_signals.get(f"{tf}_SHORT", {}).get('signal', 'None'))
                        
                        symbol_data.append([
                            symbol,
                            tf,
                            f"{rsi_current:.2f}",
                            f"{rsi_div}",
                            f"{macd_hist:.4f}",
                            macd_cross,
                            signal,
                            f"{current_price:.4f}"
                        ])
                
                return {
                    'symbol_data': symbol_data,
                    'signals': timeframe_signals
                }
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                return None
        
        try:
            # Get batch market data first
            market_data = self.get_market_data_batch(Config.SYMBOLS)
            for symbol, data in market_data.items():
                self.market_data_cache.set(symbol, data)
            
            # Process symbols in parallel
            futures = [
                self.thread_pool.submit(process_symbol, symbol)
                for symbol in Config.SYMBOLS
            ]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        status_data.extend(result['symbol_data'])
                        
                        # Process trading signals if conditions are met
                        signals = result['signals']
                        if signals and len(self.open_trades) < Config.MAX_OPEN_TRADES:
                            best_signal = max(signals.values(), key=lambda x: x['strength'])
                            if best_signal['strength'] > 0.7:
                                symbol = result['symbol_data'][0][0]  # Get symbol from data
                                signal = best_signal['signal']
                                quantity = self.get_position_size(symbol)
                                if quantity and self.check_trade_conditions(symbol, signal):
                                    side = 'BUY' if signal == 'LONG' else 'SELL'
                                    self.place_trade(symbol, side, quantity)
                        
                except Exception as e:
                    logger.error(f"Error completing future: {e}")
            
            # Display results
            if status_data:
                table = tabulate(
                    status_data,
                    headers=['Symbol', 'TF', 'RSI', 'RSI Div', 'MACD Hist', 'MACD Cross', 'Signal', 'Price'],
                    tablefmt='grid'
                )
                print("\nMarket Analysis:")
                print(table)
                
        except Exception as e:
            logger.error(f"Error in check_signals: {e}")

        

    def synchronize_trades(self):
        """Synchronize internal trade tracking with actual positions"""
        try:
            # Get all current positions
            positions = {
                pos['symbol']: self.get_current_position(pos['symbol'])
                for pos in self.client.futures_position_information()
                if float(pos['positionAmt']) != 0
            }
            
            # Remove tracked trades that no longer exist
            for symbol in list(self.open_trades.keys()):
                if symbol not in positions:
                    logger.warning(f"Removing phantom trade for {symbol}")
                    del self.open_trades[symbol]
            
            # Add missing trades to tracking
            for symbol, position in positions.items():
                if symbol not in self.open_trades:
                    logger.warning(f"Adding missing trade for {symbol}")
                    self.open_trades[symbol] = {
                        'side': position['side'],
                        'entry_price': position['entry_price'],
                        'quantity': position['quantity'],
                        'entry_time': datetime.now() - timedelta(minutes=1),  # Approximate
                        'order_id': None,
                        'highest_price': position['entry_price'],
                        'lowest_price': position['entry_price']
                    }
                    
        except Exception as e:
            logger.error(f"Error synchronizing trades: {e}")
                

            
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

    def send_status_update(self):
        """Send periodic status update with trading statistics"""
        try:
            # Get account information
            account = self.client.futures_account()
            balance = float(account['totalWalletBalance'])
            
            # Calculate trading statistics
            win_rate = self.stats.get_win_rate() * 100
            total_trades = self.stats.total_trades
            
            # Format status message
            status_message = (
                "📊 Trading Bot Status Update\n"
                f"Balance: {balance:.2f} USDT\n"
                f"Total Trades: {total_trades}\n"
                f"Win Rate: {win_rate:.1f}%\n"
                f"Open Positions: {len(self.open_trades)}\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            # Send status via Telegram
            self.send_telegram_message(status_message)
            logger.info("Status update sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending status update: {e}")                


    def run(self):
        """Main bot loop with improved performance"""
        self.running = True
        error_count = 0
        max_consecutive_errors = 5
        last_status_update = time()
        last_sync = time()
        last_market_update = time()
        
        status_update_interval = 1800  # 30 minutes in seconds
        sync_interval = 300  # 5 minutes in seconds
        market_update_interval = 5  # 5 seconds
        
        logger.info("Bot starting...")
        self.loop.run_until_complete(self.send_telegram("🟢 Trading Bot Started"))
        
        try:
            while self.running:
                try:
                    current_time = time()
                    
                    # Fast updates (every iteration)
                    self.manage_trades()
                    
                    # Market data updates (every 5 seconds)
                    if current_time - last_market_update >= market_update_interval:
                        self.get_market_data_batch(Config.SYMBOLS)
                        self.check_signals()
                        self.display_open_trades()
                        last_market_update = current_time
                    
                    # Position sync (every 5 minutes)
                    if current_time - last_sync >= sync_interval:
                        self.synchronize_trades()
                        last_sync = current_time
                    
                    # Status updates (every 30 minutes)
                    if current_time - last_status_update >= status_update_interval:
                        self.send_status_update()
                        last_status_update = current_time
                    
                    error_count = 0
                    sleep(0.1)  # Reduced sleep time for more responsiveness
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    
                    if error_count >= max_consecutive_errors:
                        logger.critical(f"Stopping bot due to {error_count} consecutive errors")
                        self.loop.run_until_complete(self.send_telegram(
                            f"🛑 Bot stopped after {error_count} consecutive errors"
                        ))
                        break
                    
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            logger.info("Bot manually stopped")
            self.loop.run_until_complete(self.send_telegram("🔴 Bot Manually Stopped"))
            
        finally:
            self._save_trading_history()
            self.thread_pool.shutdown()  # Clean up thread pool

if __name__ == "__main__":
    try:
        logger = setup_logging()
        bot = TradingBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Bot manually stopped")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)