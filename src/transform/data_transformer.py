import pandas as pd
import numpy as np
from datetime import datetime

class DataTransformer:
    
    def transform_stock_data(self, df):
        """Transform stock price data"""
        if df.empty:
            return df
        
        # 1. Data type conversions
        df['date'] = pd.to_datetime(df['date'])
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. Handle missing values
        df = df.dropna(subset=['close'])
        df['volume'] = df['volume'].fillna(0)
        
        # 3. Feature engineering
        df['daily_return'] = df.groupby('symbol')['close'].pct_change()
        df['price_range'] = df['high'] - df['low']
        df['price_change'] = df['close'] - df['open']
        df['price_change_pct'] = (df['price_change'] / df['open']) * 100
        
        # 4. Add technical indicators
        df = self._add_moving_averages(df)
        df = self._add_volatility(df)
        
        # 5. Data validation
        df = df[df['close'] > 0]
        df = df[df['volume'] >= 0]
        
        # 6. Remove duplicates
        df = df.drop_duplicates(subset=['symbol', 'date'])
        
        # 7. Add metadata
        df['transformed_at'] = datetime.now()
        
        print(f"✅ Transformed {len(df)} stock records")
        return df
    
    def _add_moving_averages(self, df):
        """Add moving averages"""
        for window in [7, 30]:
            df[f'ma_{window}'] = df.groupby('symbol')['close'].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
        return df
    
    def _add_volatility(self, df):
        """Add volatility metrics"""
        df['volatility_30d'] = df.groupby('symbol')['daily_return'].transform(
            lambda x: x.rolling(window=30, min_periods=1).std()
        )
        return df
    
    def transform_crypto_data(self, df):
        """Transform crypto price data"""
        if df.empty:
            return df
        
        # 1. Data type conversions
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
        
        # 2. Handle missing values
        df = df.dropna(subset=['price_usd'])
        
        # 3. Feature engineering
        df['price_category'] = pd.cut(
            df['price_usd'],
            bins=[0, 100, 1000, 10000, float('inf')],
            labels=['low', 'medium', 'high', 'very_high']
        )
        
        # 4. Add metadata
        df['transformed_at'] = datetime.now()
        
        print(f"✅ Transformed {len(df)} crypto records")
        return df
    
    def transform_news_data(self, df):
        """Transform news data"""
        if df.empty:
            return df
        
        # 1. Clean text
        df['title'] = df['title'].str.strip()
        df['title_length'] = df['title'].str.len()
        
        # 2. Extract keywords (simple version)
        keywords = ['earnings', 'merger', 'acquisition', 'revenue', 'profit', 
                   'loss', 'growth', 'decline', 'bullish', 'bearish']
        
        for keyword in keywords:
            df[f'has_{keyword}'] = df['title'].str.lower().str.contains(keyword, na=False)
        
        # 3. Remove duplicates
        df = df.drop_duplicates(subset=['title', 'symbol'])
        
        # 4. Add metadata
        df['transformed_at'] = datetime.now()
        
        print(f"✅ Transformed {len(df)} news records")
        return df
    
    def transform_portfolio_data(self, df):
        """Transform portfolio data"""
        if df.empty:
            return df
        
        # 1. Data type conversions
        df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['purchase_price'] = pd.to_numeric(df['purchase_price'], errors='coerce')
        
        # 2. Calculate metrics
        df['cost_basis'] = df['quantity'] * df['purchase_price']
        df['holding_days'] = (datetime.now() - df['purchase_date']).dt.days
        
        # 3. Add metadata
        df['transformed_at'] = datetime.now()
        
        print(f"✅ Transformed {len(df)} portfolio records")
        return df

# Test
if __name__ == "__main__":
    # Create sample data
    sample_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10),
        'symbol': ['AAPL'] * 10,
        'open': np.random.uniform(150, 160, 10),
        'high': np.random.uniform(160, 170, 10),
        'low': np.random.uniform(145, 155, 10),
        'close': np.random.uniform(150, 165, 10),
        'volume': np.random.randint(1000000, 5000000, 10),
        'extraction_timestamp': datetime.now()
    })
    
    transformer = DataTransformer()
    transformed = transformer.transform_stock_data(sample_data)
    print(transformed.head())