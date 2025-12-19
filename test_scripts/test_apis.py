"""
Test script for CoinGecko and Reddit API access
Verifies API connectivity and explores response structure
"""

import requests
import json
from pprint import pprint

def test_coingecko_api():
    """Test CoinGecko API - no authentication required"""
    print("=" * 60)
    print("Testing CoinGecko API")
    print("=" * 60)
    
    # Endpoint 1: Get list of top coins by market cap
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,  # Start with just 10 for testing
        'page': 1,
        'sparkline': False
    }
    
    try:
        print("\nFetching top 10 coins by market cap...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        print(f"[OK] Successfully fetched {len(data)} coins")
        
        # Show first coin as example
        if data:
            print("\nExample coin data (Bitcoin):")
            print("-" * 60)
            example = data[0]
            print(f"ID: {example['id']}")
            print(f"Symbol: {example['symbol']}")
            print(f"Name: {example['name']}")
            print(f"Current Price: ${example['current_price']:,.2f}")
            print(f"Market Cap: ${example['market_cap']:,.0f}")
            print(f"24h Volume: ${example['total_volume']:,.0f}")
            print(f"24h Price Change %: {example['price_change_percentage_24h']:.2f}%")
            print(f"Last Updated: {example['last_updated']}")
            
            # Show all available fields
            print("\nAll available fields:")
            print(json.dumps(list(example.keys()), indent=2))
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[FAIL] CoinGecko API request failed: {e}")
        return False

def test_coingecko_ping():
    """Test CoinGecko API status"""
    try:
        print("\n" + "-" * 60)
        print("Testing CoinGecko API ping...")
        response = requests.get("https://api.coingecko.com/api/v3/ping")
        response.raise_for_status()
        data = response.json()
        print(f"[OK] API Status: {data}")
        return True
    except Exception as e:
        print(f"[FAIL] Ping failed: {e}")
        return False

def test_feargreed_api():
    """Test Crypto Fear & Greed Index API - no authentication required"""
    print("\n" + "=" * 60)
    print("Testing Fear & Greed Index API")
    print("=" * 60)
    
    try:
        print("\nFetching current Fear & Greed Index...")
        
        # Get current index value
        url = "https://api.alternative.me/fng/"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('data'):
            current = data['data'][0]
            print(f"[OK] Successfully fetched Fear & Greed data")
            
            print("\nCurrent Fear & Greed Index:")
            print("-" * 60)
            print(f"Value: {current['value']}")
            print(f"Classification: {current['value_classification']}")
            print(f"Timestamp: {current['timestamp']}")
            
            # Get historical data (last 7 days)
            print("\nFetching historical data (last 7 days)...")
            url_historical = "https://api.alternative.me/fng/?limit=7"
            response_hist = requests.get(url_historical)
            response_hist.raise_for_status()
            
            hist_data = response_hist.json()
            if hist_data.get('data'):
                print(f"[OK] Successfully fetched {len(hist_data['data'])} historical records")
                
                print("\nLast 3 days:")
                for record in hist_data['data'][:3]:
                    print(f"  {record['timestamp']}: {record['value']} ({record['value_classification']})")
                
                print("\nAll available fields:")
                print(json.dumps(list(current.keys()), indent=2))
            
            return True
        else:
            print("[FAIL] No data returned from API")
            return False
            
    except Exception as e:
        print(f"[FAIL] Fear & Greed API request failed: {e}")
        return False

def document_endpoints():
    """Document which endpoints we'll use in production"""
    print("\n" + "=" * 60)
    print("Endpoints for Production Pipeline")
    print("=" * 60)
    
    print("\nCoinGecko Endpoints:")
    print("-" * 60)
    print("1. /coins/markets")
    print("   Purpose: Get price, volume, market cap for top coins")
    print("   Rate limit: 10-50 calls/min (free tier)")
    print("   Params: vs_currency=usd, per_page=50, order=market_cap_desc")
    
    print("\nFear & Greed Index Endpoints:")
    print("-" * 60)
    print("1. https://api.alternative.me/fng/")
    print("   Purpose: Get crypto market sentiment (0-100 scale)")
    print("   Rate limit: No official limit (be reasonable)")
    print("   No authentication required")
    print("   Updates: Daily")
    print("   Historical: Add ?limit=N parameter for last N days")
    print("\nNote: Simple daily sentiment metric that complements price data")

if __name__ == "__main__":
    print("\n" + "#" * 60)
    print("# API Testing Script")
    print("#" * 60)
    
    # Test CoinGecko
    coingecko_ok = test_coingecko_ping()
    if coingecko_ok:
        coingecko_ok = test_coingecko_api()
    
    # Test Fear & Greed Index
    feargreed_ok = test_feargreed_api()
    
    # Document endpoints
    document_endpoints()
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"CoinGecko: {'[OK]' if coingecko_ok else '[FAIL]'}")
    print(f"Fear & Greed Index: {'[OK]' if feargreed_ok else '[FAIL]'}")
    
    if coingecko_ok and feargreed_ok:
        print("\n[OK] All APIs are ready to use!")
    else:
        print("\n[ACTION NEEDED] Check failed API tests above")
    
    print("=" * 60)