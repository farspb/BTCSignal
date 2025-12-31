"""
Bitcoin data fetching module with support for multiple APIs and caching.

This module provides functions to fetch real-time Bitcoin data from CoinGecko
and CoinMarketCap free APIs, with support for different timeframes and caching.
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import hashlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Configuration
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINMARKETCAP_BASE_URL = "https://pro-api.coinmarketcap.com/v1"

# Cache configuration
CACHE_DIR = Path("cache")
CACHE_EXPIRY_MINUTES = 5  # Cache validity in minutes

# Timeframe mapping for different APIs
TIMEFRAMES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "7d": 10080,
    "30d": 43200,
}


class DataCache:
    """Simple file-based cache for API responses."""

    def __init__(self, cache_dir: Path = CACHE_DIR):
        """Initialize cache directory."""
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_path(self, key: str) -> Path:
        """Generate cache file path from key."""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hash_key}.json"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached data if still valid."""
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, 'r') as f:
                cached_data = json.load(f)

            # Check if cache is still valid
            timestamp = cached_data.get('timestamp', 0)
            age_minutes = (time.time() - timestamp) / 60
            
            if age_minutes < CACHE_EXPIRY_MINUTES:
                logger.info(f"Cache hit for key: {key}")
                return cached_data.get('data')
            else:
                logger.info(f"Cache expired for key: {key}")
                return None
        except Exception as e:
            logger.warning(f"Error reading cache: {e}")
            return None

    def set(self, key: str, data: Dict[str, Any]) -> None:
        """Store data in cache."""
        cache_path = self._get_cache_path(key)
        
        try:
            cache_data = {
                'timestamp': time.time(),
                'data': data
            }
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f)
            logger.info(f"Cached data for key: {key}")
        except Exception as e:
            logger.warning(f"Error writing cache: {e}")

    def clear(self) -> None:
        """Clear all cached data."""
        try:
            for file in self.cache_dir.glob("*.json"):
                file.unlink()
            logger.info("Cache cleared")
        except Exception as e:
            logger.warning(f"Error clearing cache: {e}")


# Global cache instance
cache = DataCache()


class CoinGeckoFetcher:
    """Fetch Bitcoin data from CoinGecko API."""

    def __init__(self):
        """Initialize CoinGecko fetcher."""
        self.base_url = COINGECKO_BASE_URL
        self.session = requests.Session()

    def get_current_price(self) -> Optional[Dict[str, Any]]:
        """
        Fetch current Bitcoin price from CoinGecko.

        Returns:
            Dictionary with price data or None if request fails.
        """
        cache_key = "coingecko_current_price"
        
        # Check cache first
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/simple/price"
            params = {
                "ids": "bitcoin",
                "vs_currencies": "usd,eur,gbp",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coingecko",
                "timestamp": datetime.utcnow().isoformat(),
                "data": data.get("bitcoin", {})
            }
            
            cache.set(cache_key, result)
            logger.info("Successfully fetched current price from CoinGecko")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from CoinGecko: {e}")
            return None

    def get_market_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed market data for Bitcoin.

        Returns:
            Dictionary with market data or None if request fails.
        """
        cache_key = "coingecko_market_data"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/coins/bitcoin"
            params = {
                "localization": "false",
                "tickers": "false",
                "market_data": "true",
                "community_data": "false",
                "developer_data": "false"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coingecko",
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    "id": data.get("id"),
                    "symbol": data.get("symbol"),
                    "name": data.get("name"),
                    "market_data": data.get("market_data", {})
                }
            }
            
            cache.set(cache_key, result)
            logger.info("Successfully fetched market data from CoinGecko")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching market data from CoinGecko: {e}")
            return None

    def get_historical_data(self, days: int = 90) -> Optional[Dict[str, Any]]:
        """
        Fetch historical Bitcoin price data.

        Args:
            days: Number of days of historical data to fetch (max 365).

        Returns:
            Dictionary with historical data or None if request fails.
        """
        days = min(days, 365)  # Limit to 365 days
        cache_key = f"coingecko_historical_{days}d"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/coins/bitcoin/market_chart"
            params = {
                "vs_currency": "usd",
                "days": days,
                "interval": "daily"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coingecko",
                "timestamp": datetime.utcnow().isoformat(),
                "days": days,
                "prices": data.get("prices", []),
                "market_caps": data.get("market_caps", []),
                "volumes": data.get("volumes", [])
            }
            
            cache.set(cache_key, result)
            logger.info(f"Successfully fetched {days} days of historical data from CoinGecko")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching historical data from CoinGecko: {e}")
            return None


class CoinMarketCapFetcher:
    """Fetch Bitcoin data from CoinMarketCap API (free tier)."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize CoinMarketCap fetcher.

        Args:
            api_key: CoinMarketCap API key (optional, uses free tier if not provided).
        """
        self.base_url = COINMARKETCAP_BASE_URL
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"X-CMC_PRO_API_KEY": api_key})

    def get_current_price(self) -> Optional[Dict[str, Any]]:
        """
        Fetch current Bitcoin price from CoinMarketCap.

        Returns:
            Dictionary with price data or None if request fails.
        """
        cache_key = "coinmarketcap_current_price"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/cryptocurrency/quotes/latest"
            params = {
                "symbol": "BTC",
                "convert": "USD,EUR,GBP"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coinmarketcap",
                "timestamp": datetime.utcnow().isoformat(),
                "data": data.get("data", {}).get("BTC", {})
            }
            
            cache.set(cache_key, result)
            logger.info("Successfully fetched current price from CoinMarketCap")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from CoinMarketCap: {e}")
            return None

    def get_cryptocurrency_info(self) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed cryptocurrency information for Bitcoin.

        Returns:
            Dictionary with cryptocurrency info or None if request fails.
        """
        cache_key = "coinmarketcap_info"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/cryptocurrency/info"
            params = {"symbol": "BTC"}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coinmarketcap",
                "timestamp": datetime.utcnow().isoformat(),
                "data": data.get("data", {}).get("BTC", {})
            }
            
            cache.set(cache_key, result)
            logger.info("Successfully fetched info from CoinMarketCap")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching info from CoinMarketCap: {e}")
            return None

    def get_global_metrics(self) -> Optional[Dict[str, Any]]:
        """
        Fetch global market metrics.

        Returns:
            Dictionary with global metrics or None if request fails.
        """
        cache_key = "coinmarketcap_global_metrics"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.base_url}/global-metrics/quotes/latest"
            params = {"convert": "USD"}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            result = {
                "source": "coinmarketcap",
                "timestamp": datetime.utcnow().isoformat(),
                "data": data.get("data", {})
            }
            
            cache.set(cache_key, result)
            logger.info("Successfully fetched global metrics from CoinMarketCap")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching global metrics from CoinMarketCap: {e}")
            return None


class TimeframeDataHandler:
    """Handle Bitcoin data aggregation for different timeframes."""

    def __init__(self):
        """Initialize timeframe handler."""
        self.coingecko = CoinGeckoFetcher()

    def get_ohlcv_data(self, timeframe: str = "1d") -> Optional[Dict[str, Any]]:
        """
        Get OHLCV (Open, High, Low, Close, Volume) data for specified timeframe.

        Args:
            timeframe: Timeframe string (1m, 5m, 15m, 30m, 1h, 4h, 1d, 7d, 30d).

        Returns:
            Dictionary with OHLCV data or None if timeframe is invalid.
        """
        if timeframe not in TIMEFRAMES:
            logger.error(f"Invalid timeframe: {timeframe}")
            return None

        cache_key = f"ohlcv_{timeframe}"
        
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            # For CoinGecko, use market chart data
            if timeframe in ["1d", "7d", "30d"]:
                days = {
                    "1d": 1,
                    "7d": 7,
                    "30d": 30
                }.get(timeframe, 1)
                
                data = self.coingecko.get_historical_data(days=days)
                
                if data:
                    result = {
                        "source": "coingecko",
                        "timeframe": timeframe,
                        "timestamp": datetime.utcnow().isoformat(),
                        "ohlcv": self._process_historical_to_ohlcv(data)
                    }
                    cache.set(cache_key, result)
                    return result
            
            # For smaller timeframes, we'd need real-time data from a different source
            logger.warning(f"Timeframe {timeframe} not fully supported with free APIs")
            return None
            
        except Exception as e:
            logger.error(f"Error processing OHLCV data: {e}")
            return None

    @staticmethod
    def _process_historical_to_ohlcv(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert historical price data to OHLCV format.

        Args:
            data: Historical data from CoinGecko.

        Returns:
            List of OHLCV candles.
        """
        prices = data.get("prices", [])
        volumes = data.get("volumes", [])
        
        ohlcv = []
        
        for i, (timestamp, price) in enumerate(prices):
            volume = volumes[i][1] if i < len(volumes) else 0
            
            ohlcv.append({
                "timestamp": datetime.fromtimestamp(timestamp / 1000).isoformat(),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            })
        
        return ohlcv

    def aggregate_to_timeframe(self, prices: List[Tuple[int, float]], 
                              timeframe: str) -> List[Dict[str, Any]]:
        """
        Aggregate price data to specified timeframe.

        Args:
            prices: List of (timestamp, price) tuples.
            timeframe: Target timeframe.

        Returns:
            List of aggregated OHLCV data.
        """
        if timeframe not in TIMEFRAMES:
            logger.error(f"Invalid timeframe: {timeframe}")
            return []

        timeframe_minutes = TIMEFRAMES[timeframe]
        timeframe_seconds = timeframe_minutes * 60

        aggregated = {}
        
        for timestamp, price in prices:
            # Round down timestamp to timeframe boundary
            bucket = (timestamp // timeframe_seconds) * timeframe_seconds
            
            if bucket not in aggregated:
                aggregated[bucket] = {
                    "timestamp": bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0,
                    "count": 0
                }
            else:
                aggregated[bucket]["high"] = max(aggregated[bucket]["high"], price)
                aggregated[bucket]["low"] = min(aggregated[bucket]["low"], price)
                aggregated[bucket]["close"] = price
            
            aggregated[bucket]["count"] += 1

        return sorted(aggregated.values(), key=lambda x: x["timestamp"])


class DataFetcher:
    """Main data fetcher class combining all sources."""

    def __init__(self, coinmarketcap_api_key: Optional[str] = None):
        """
        Initialize DataFetcher.

        Args:
            coinmarketcap_api_key: Optional CoinMarketCap API key.
        """
        self.coingecko = CoinGeckoFetcher()
        self.coinmarketcap = CoinMarketCapFetcher(api_key=coinmarketcap_api_key)
        self.timeframe_handler = TimeframeDataHandler()

    def get_bitcoin_price(self, source: str = "coingecko") -> Optional[Dict[str, Any]]:
        """
        Get current Bitcoin price.

        Args:
            source: Data source ('coingecko' or 'coinmarketcap').

        Returns:
            Dictionary with price data.
        """
        if source == "coingecko":
            return self.coingecko.get_current_price()
        elif source == "coinmarketcap":
            return self.coinmarketcap.get_current_price()
        else:
            logger.error(f"Unknown source: {source}")
            return None

    def get_market_data(self, source: str = "coingecko") -> Optional[Dict[str, Any]]:
        """
        Get detailed market data.

        Args:
            source: Data source ('coingecko' or 'coinmarketcap').

        Returns:
            Dictionary with market data.
        """
        if source == "coingecko":
            return self.coingecko.get_market_data()
        elif source == "coinmarketcap":
            return self.coinmarketcap.get_cryptocurrency_info()
        else:
            logger.error(f"Unknown source: {source}")
            return None

    def get_historical_data(self, days: int = 90, 
                           source: str = "coingecko") -> Optional[Dict[str, Any]]:
        """
        Get historical Bitcoin data.

        Args:
            days: Number of days of historical data.
            source: Data source ('coingecko' or 'coinmarketcap').

        Returns:
            Dictionary with historical data.
        """
        if source == "coingecko":
            return self.coingecko.get_historical_data(days=days)
        else:
            logger.error(f"Historical data not available for source: {source}")
            return None

    def get_ohlcv(self, timeframe: str = "1d") -> Optional[Dict[str, Any]]:
        """
        Get OHLCV data for specified timeframe.

        Args:
            timeframe: Timeframe string.

        Returns:
            Dictionary with OHLCV data.
        """
        return self.timeframe_handler.get_ohlcv_data(timeframe)

    def get_global_metrics(self) -> Optional[Dict[str, Any]]:
        """
        Get global cryptocurrency market metrics.

        Returns:
            Dictionary with global metrics.
        """
        return self.coinmarketcap.get_global_metrics()

    def clear_cache(self) -> None:
        """Clear all cached data."""
        cache.clear()

    def get_supported_timeframes(self) -> List[str]:
        """
        Get list of supported timeframes.

        Returns:
            List of supported timeframe strings.
        """
        return list(TIMEFRAMES.keys())


# Example usage
if __name__ == "__main__":
    # Initialize fetcher
    fetcher = DataFetcher()

    # Get current price
    print("Getting current Bitcoin price...")
    price = fetcher.get_bitcoin_price()
    if price:
        print(f"Price data: {json.dumps(price, indent=2)}")

    # Get market data
    print("\nGetting market data...")
    market = fetcher.get_market_data()
    if market:
        print(f"Market data retrieved at {market.get('timestamp')}")

    # Get historical data
    print("\nGetting 30-day historical data...")
    historical = fetcher.get_historical_data(days=30)
    if historical:
        print(f"Historical data points: {len(historical.get('prices', []))}")

    # Get OHLCV data
    print("\nGetting OHLCV data for 1d timeframe...")
    ohlcv = fetcher.get_ohlcv(timeframe="1d")
    if ohlcv:
        print(f"OHLCV candles: {len(ohlcv.get('ohlcv', []))}")

    # Get supported timeframes
    print(f"\nSupported timeframes: {fetcher.get_supported_timeframes()}")
