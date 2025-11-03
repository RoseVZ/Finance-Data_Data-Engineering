import requests
import pandas as pd
from datetime import datetime
import time

class CryptoDataExtractor:
    def __init__(self):
        self.base_url = 'https://api.coingecko.com/api/v3'
    
    def get_current_prices(self, crypto_ids):
        """Get current prices for cryptocurrencies"""
        try:
            params = {
                'ids': ','.join(crypto_ids),
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true'
            }
            
            response = requests.get(
                f'{self.base_url}/simple/price',
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Convert to DataFrame
            records = []
            for crypto_id, values in data.items():
                records.append({
                    'crypto_id': crypto_id,
                    'price_usd': values.get('usd', 0),
                    'market_cap': values.get('usd_market_cap', 0),
                    'volume_24h': values.get('usd_24h_vol', 0),
                    'change_24h': values.get('usd_24h_change', 0),
                    'timestamp': datetime.now()
                })
            
            df = pd.DataFrame(records)
            print(f"✅ Extracted data for {len(df)} cryptocurrencies")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting crypto data: {str(e)}")
            return pd.DataFrame()
    
    def get_historical_prices(self, crypto_id, days=30):
        """Get historical prices"""
        try:
            params = {
                'vs_currency': 'usd',
                'days': days
            }
            
            response = requests.get(
                f'{self.base_url}/coins/{crypto_id}/market_chart',
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract prices
            prices = data['prices']
            df = pd.DataFrame(prices, columns=['timestamp', 'price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['crypto_id'] = crypto_id
            
            print(f"✅ Extracted {len(df)} historical records for {crypto_id}")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting historical data for {crypto_id}: {str(e)}")
            return pd.DataFrame()

# Test
if __name__ == "__main__":
    extractor = CryptoDataExtractor()
    df = extractor.get_current_prices(['bitcoin', 'ethereum'])
    print(df)