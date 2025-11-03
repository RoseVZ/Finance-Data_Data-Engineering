
import sys
sys.path.append('src')

from extract.stock_api import StockDataExtractor
from extract.crypto_api import CryptoDataExtractor
from extract.news_scraper import NewsScraper
from extract.portfolio_db import PortfolioDatabase
from transform.data_transformer import DataTransformer
from load.bigquery_loader import BigQueryLoader
from dotenv import load_dotenv
import os
import yaml

load_dotenv()

def test_etl():
    """Test complete ETL flow"""
    
    print("=" * 60)
    print("TESTING ETL PIPELINE")
    print("=" * 60)
    
    # Load config
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize components
    stock_extractor = StockDataExtractor()
    crypto_extractor = CryptoDataExtractor()
    news_scraper = NewsScraper()
    transformer = DataTransformer()
    
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    loader = BigQueryLoader(project_id, 'financial_data')
    
    # 1. EXTRACT
    print("\n" + "=" * 60)
    print("PHASE 1: EXTRACTION")
    print("=" * 60)
    
    # Extract stock data (just 1 symbol for testing)
    print("\n📊 Extracting stock data...")
    stock_df = stock_extractor.get_daily_prices('AAPL')
    print(f"Extracted: {len(stock_df)} stock records")
    
    # Extract crypto data
    print("\n💰 Extracting crypto data...")
    crypto_df = crypto_extractor.get_current_prices(['bitcoin', 'ethereum'])
    print(f"Extracted: {len(crypto_df)} crypto records")
    
    # Extract news
    print("\n📰 Extracting news...")
    news_df = news_scraper.scrape_yahoo_finance('AAPL')
    print(f"Extracted: {len(news_df)} news articles")
    
    # Extract portfolio (if DB is set up)
    print("\n💼 Extracting portfolio data...")
    try:
        db = PortfolioDatabase(config['database'])
        db.connect()
        portfolio_df = db.extract_portfolio_data()
        db.close()
        print(f"Extracted: {len(portfolio_df)} portfolio records")
    except Exception as e:
        print(f"⚠️  Portfolio extraction skipped: {str(e)}")
        portfolio_df = None
    
    # 2. TRANSFORM
    print("\n" + "=" * 60)
    print("PHASE 2: TRANSFORMATION")
    print("=" * 60)
    
    print("\n🔄 Transforming stock data...")
    stock_transformed = transformer.transform_stock_data(stock_df)
    
    print("\n🔄 Transforming crypto data...")
    crypto_transformed = transformer.transform_crypto_data(crypto_df)
    
    print("\n🔄 Transforming news data...")
    news_transformed = transformer.transform_news_data(news_df)
    
    if portfolio_df is not None and not portfolio_df.empty:
        print("\n🔄 Transforming portfolio data...")
        portfolio_transformed = transformer.transform_portfolio_data(portfolio_df)
    else:
        portfolio_transformed = None
    
    # 3. LOAD
    print("\n" + "=" * 60)
    print("PHASE 3: LOADING TO BIGQUERY")
    print("=" * 60)
    
    print("\n📤 Creating tables...")
    loader.create_tables()
    
    print("\n📤 Loading stock prices...")
    loader.load_stock_prices(stock_transformed)
    
    print("\n📤 Loading crypto prices...")
    loader.load_crypto_prices(crypto_transformed)
    
    print("\n📤 Loading news...")
    loader.load_news(news_transformed)
    
    if portfolio_transformed is not None:
        print("\n📤 Loading portfolio...")
        loader.load_portfolio(portfolio_transformed)
    
    # 4. VERIFY
    print("\n" + "=" * 60)
    print("PHASE 4: VERIFICATION")
    print("=" * 60)
    
    print("\n✅ Checking row counts...")
    print(f"Stock prices: {loader.get_table_row_count('stock_prices')}")
    print(f"Crypto prices: {loader.get_table_row_count('crypto_prices')}")
    print(f"Market news: {loader.get_table_row_count('market_news')}")
    print(f"User portfolio: {loader.get_table_row_count('user_portfolio')}")
    
    print("\n" + "=" * 60)
    print("✅ ETL TEST COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    test_etl()