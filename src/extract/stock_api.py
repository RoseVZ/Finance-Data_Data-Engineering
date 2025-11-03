import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

class StockDataExtractor:
    def __init__(self):
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.base_url = 'https://www.alphavantage.co/query'
    
    def get_daily_prices(self, symbol):
        """Extract daily stock prices"""
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Last 100 days
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'Time Series (Daily)' not in data:
                print(f"Error for {symbol}: {data.get('Note', data.get('Error Message', 'Unknown error'))}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            time_series = data['Time Series (Daily)']
            df = pd.DataFrame.from_dict(time_series, orient='index')
            
            # Clean column names
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            df.index.name = 'date'
            df.reset_index(inplace=True)
            
            # Add metadata
            df['symbol'] = symbol
            df['extraction_timestamp'] = datetime.now()
            
            # Convert types
            df['date'] = pd.to_datetime(df['date'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            
            print(f"✅ Extracted {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting {symbol}: {str(e)}")
            return pd.DataFrame()
    
    def get_multiple_stocks(self, symbols):
        """Extract data for multiple stocks"""
        all_data = []
        
        for symbol in symbols:
            df = self.get_daily_prices(symbol)
            if not df.empty:
                all_data.append(df)
            time.sleep(12)  # Rate limit: 5 calls/min for free tier
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

# Test function
if __name__ == "__main__":
    extractor = StockDataExtractor()
    df = extractor.get_daily_prices('AAPL')
    print(df.head())