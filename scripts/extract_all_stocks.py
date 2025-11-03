import sys
sys.path.append('src')

from extract.stock_api import StockDataExtractor
from extract.news_scraper import NewsScraper
from transform.data_transformer import DataTransformer
from load.bigquery_loader import BigQueryLoader
from dotenv import load_dotenv
import os
import yaml

load_dotenv()

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

print("=" * 60)
print("EXTRACTING ALL STOCKS")
print("=" * 60)

# Extract stocks
extractor = StockDataExtractor()
symbols = config['stocks']['symbols']

print(f"\nExtracting data for: {', '.join(symbols)}")

all_stock_data = []
for symbol in symbols:
    print(f"\n📊 Extracting {symbol}...")
    df = extractor.get_daily_prices(symbol)
    if not df.empty:
        all_stock_data.append(df)

if all_stock_data:
    import pandas as pd
    stock_df = pd.concat(all_stock_data, ignore_index=True)
    
    # Transform
    print("\n🔄 Transforming data...")
    transformer = DataTransformer()
    stock_transformed = transformer.transform_stock_data(stock_df)
    
    # Load to BigQuery
    print("\n📤 Loading to BigQuery...")
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    loader = BigQueryLoader(project_id, 'financial_data')
    loader.load_stock_prices(stock_transformed)
    
    print(f"\n✅ Successfully loaded {len(stock_transformed)} records for {len(symbols)} stocks!")

# Extract news
print("\n" + "=" * 60)
print("EXTRACTING NEWS")
print("=" * 60)

scraper = NewsScraper()
all_news = []

for symbol in symbols:
    print(f"\n📰 Scraping news for {symbol}...")
    news_df = scraper.scrape_yahoo_finance(symbol)
    if news_df.empty:
        news_df = scraper.scrape_finviz_news(symbol)
    if not news_df.empty:
        all_news.append(news_df)

if all_news:
    import pandas as pd
    news_df = pd.concat(all_news, ignore_index=True)
    
    # Transform
    print("\n🔄 Transforming news...")
    news_transformed = transformer.transform_news_data(news_df)
    
    # Load to BigQuery
    print("\n📤 Loading news to BigQuery...")
    loader.load_news(news_transformed)
    
    print(f"\n✅ Successfully loaded {len(news_transformed)} news articles!")
else:
    print("\n⚠️ No news data extracted")

print("\n" + "=" * 60)
print("✅ EXTRACTION COMPLETE!")
print("=" * 60)
