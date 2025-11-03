from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Add src to path
sys.path.insert(0, '/opt/airflow/src')

from extract.stock_api import StockDataExtractor
from extract.crypto_api import CryptoDataExtractor
from extract.news_scraper import NewsScraper
from extract.portfolio_db import PortfolioDatabase
from transform.data_transformer import DataTransformer
from load.bigquery_loader import BigQueryLoader

import yaml
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')


# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['priyarosev949@gmail.com'],  # Update this!
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Create DAG
dag = DAG(
    'financial_market_etl_pipeline',
    default_args=default_args,
    description='Automated daily financial market data ETL pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    catchup=False,
    tags=['finance', 'etl', 'bigquery', 'production'],
)

# def test_email_task():
#     """Test task that fails to trigger email"""
#     raise Exception(" TEST: This is an intentional failure to test email notifications!")

# test_email = PythonOperator(
#     task_id='test_email_notification',
#     python_callable=test_email_task,
#     dag=dag,
# )
# Load config
with open('/opt/airflow/config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Temp directory
TEMP_DIR = '/tmp/financial_etl'

def setup_temp_dir():
    """Create temporary directory"""
    os.makedirs(TEMP_DIR, exist_ok=True)
    print(f"✅ Created temp directory: {TEMP_DIR}")

# Task 0: Setup
setup_task = PythonOperator(
    task_id='setup_temp_directory',
    python_callable=setup_temp_dir,
    dag=dag,
)

# Task 1: Extract Stock Data
def extract_stock_data(**context):
    """Extract stock data from Alpha Vantage API"""
    print("=" * 70)
    print("📊 EXTRACTING STOCK DATA")
    print("=" * 70)
    
    extractor = StockDataExtractor()
    symbols = config['stocks']['symbols']
    
    print(f"\nTarget symbols: {', '.join(symbols)}")
    
    stock_df = extractor.get_multiple_stocks(symbols)
    
    if stock_df.empty:
        raise ValueError("❌ No stock data extracted!")
    
    filepath = f"{TEMP_DIR}/stock_data.csv"
    stock_df.to_csv(filepath, index=False)
    
    print(f"\n✅ SUCCESS: Extracted {len(stock_df)} records for {len(stock_df['symbol'].unique())} symbols")
    print(f"📁 Saved to: {filepath}")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='stock_count', value=len(stock_df))
    context['task_instance'].xcom_push(key='stock_symbols', value=stock_df['symbol'].unique().tolist())

extract_stocks_task = PythonOperator(
    task_id='extract_stock_data',
    python_callable=extract_stock_data,
    dag=dag,
)

# Task 2: Extract Crypto Data
def extract_crypto_data(**context):
    """Extract crypto data from CoinGecko API"""
    print("=" * 70)
    print("💰 EXTRACTING CRYPTO DATA")
    print("=" * 70)
    
    extractor = CryptoDataExtractor()
    crypto_ids = config['crypto']['symbols']
    
    print(f"\nTarget cryptocurrencies: {', '.join(crypto_ids)}")
    
    crypto_df = extractor.get_current_prices(crypto_ids)
    
    if crypto_df.empty:
        raise ValueError("❌ No crypto data extracted!")
    
    filepath = f"{TEMP_DIR}/crypto_data.csv"
    crypto_df.to_csv(filepath, index=False)
    
    print(f"\n✅ SUCCESS: Extracted {len(crypto_df)} crypto records")
    print(f"📁 Saved to: {filepath}")
    
    context['task_instance'].xcom_push(key='crypto_count', value=len(crypto_df))

extract_crypto_task = PythonOperator(
    task_id='extract_crypto_data',
    python_callable=extract_crypto_data,
    dag=dag,
)

# Task 3: Extract News
def extract_news_data(**context):
    """Scrape news from financial websites"""
    print("=" * 70)
    print("📰 EXTRACTING NEWS DATA")
    print("=" * 70)
    
    scraper = NewsScraper()
    symbols = config['stocks']['symbols'][:3]  # Top 3 stocks for news
    
    print(f"\nScraping news for: {', '.join(symbols)}")
    
    all_news = []
    for symbol in symbols:
        print(f"\n  Scraping {symbol}...")
        news_df = scraper.scrape_yahoo_finance(symbol)
        if news_df.empty:
            news_df = scraper.scrape_finviz_news(symbol)
        if not news_df.empty:
            all_news.append(news_df)
            print(f"  ✅ Found {len(news_df)} articles for {symbol}")
    
    if all_news:
        news_df = pd.concat(all_news, ignore_index=True)
    else:
        news_df = pd.DataFrame()
    
    filepath = f"{TEMP_DIR}/news_data.csv"
    news_df.to_csv(filepath, index=False)
    
    print(f"\n✅ SUCCESS: Extracted {len(news_df)} news articles")
    print(f"📁 Saved to: {filepath}")
    
    context['task_instance'].xcom_push(key='news_count', value=len(news_df))

extract_news_task = PythonOperator(
    task_id='extract_news_data',
    python_callable=extract_news_data,
    dag=dag,
)

# Task 4: Extract Portfolio
def extract_portfolio_data(**context):
    """Extract portfolio data from PostgreSQL"""
    print("=" * 70)
    print("💼 EXTRACTING PORTFOLIO DATA")
    print("=" * 70)
    
    try:
        db = PortfolioDatabase(config['database'])
        db.connect()
        portfolio_df = db.extract_portfolio_data()
        db.close()
        
        filepath = f"{TEMP_DIR}/portfolio_data.csv"
        portfolio_df.to_csv(filepath, index=False)
        
        print(f"\n✅ SUCCESS: Extracted {len(portfolio_df)} portfolio records")
        print(f"📁 Saved to: {filepath}")
        
        context['task_instance'].xcom_push(key='portfolio_count', value=len(portfolio_df))
    except Exception as e:
        print(f"\n⚠️  WARNING: Portfolio extraction failed: {str(e)}")
        # Create empty file
        pd.DataFrame().to_csv(f"{TEMP_DIR}/portfolio_data.csv", index=False)
        context['task_instance'].xcom_push(key='portfolio_count', value=0)

extract_portfolio_task = PythonOperator(
    task_id='extract_portfolio_data',
    python_callable=extract_portfolio_data,
    dag=dag,
)

# Task 5: Transform All Data
def transform_all_data(**context):
    """Transform all extracted data"""
    print("=" * 70)
    print("🔄 TRANSFORMING DATA")
    print("=" * 70)
    
    transformer = DataTransformer()
    
    # Transform stock data
    print("\n  Transforming stock data...")
    stock_df = pd.read_csv(f"{TEMP_DIR}/stock_data.csv")
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    stock_df['extraction_timestamp'] = pd.to_datetime(stock_df['extraction_timestamp'])
    stock_transformed = transformer.transform_stock_data(stock_df)
    stock_transformed.to_csv(f"{TEMP_DIR}/stock_transformed.csv", index=False)
    
    # Transform crypto data
    print("  Transforming crypto data...")
    crypto_df = pd.read_csv(f"{TEMP_DIR}/crypto_data.csv")
    crypto_df['timestamp'] = pd.to_datetime(crypto_df['timestamp'])
    crypto_transformed = transformer.transform_crypto_data(crypto_df)
    crypto_transformed.to_csv(f"{TEMP_DIR}/crypto_transformed.csv", index=False)
    
    # Transform news data
    print("  Transforming news data...")
    news_df = pd.read_csv(f"{TEMP_DIR}/news_data.csv")
    if not news_df.empty:
        news_df['scraped_at'] = pd.to_datetime(news_df['scraped_at'])
        news_transformed = transformer.transform_news_data(news_df)
        news_transformed.to_csv(f"{TEMP_DIR}/news_transformed.csv", index=False)
    
    # Transform portfolio data
    print("  Transforming portfolio data...")
    try:
        portfolio_df = pd.read_csv(f"{TEMP_DIR}/portfolio_data.csv")
        if not portfolio_df.empty:
            portfolio_df['purchase_date'] = pd.to_datetime(portfolio_df['purchase_date'])
            portfolio_df['created_at'] = pd.to_datetime(portfolio_df['created_at'])
            portfolio_df['extraction_timestamp'] = pd.to_datetime(portfolio_df['extraction_timestamp'])
            portfolio_transformed = transformer.transform_portfolio_data(portfolio_df)
            portfolio_transformed.to_csv(f"{TEMP_DIR}/portfolio_transformed.csv", index=False)
    except Exception as e:
        print(f"  ⚠️  No portfolio data to transform: {str(e)}")
    
    print("\n✅ SUCCESS: All data transformed")

transform_task = PythonOperator(
    task_id='transform_all_data',
    python_callable=transform_all_data,
    dag=dag,
)

# Task 6: Load to BigQuery
def load_to_bigquery(**context):
    """Load all transformed data to BigQuery"""
    print("=" * 70)
    print("📤 LOADING TO BIGQUERY")
    print("=" * 70)
    
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    loader = BigQueryLoader(project_id, 'financial_data')
    
    total_loaded = 0
    
    # Load stock data
    print("\n  Loading stock data...")
    stock_df = pd.read_csv(f"{TEMP_DIR}/stock_transformed.csv")
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    stock_df['extraction_timestamp'] = pd.to_datetime(stock_df['extraction_timestamp'])
    stock_df['transformed_at'] = pd.to_datetime(stock_df['transformed_at'])
    stock_loaded = loader.load_stock_prices(stock_df)
    total_loaded += stock_loaded
    
    # Load crypto data
    print("  Loading crypto data...")
    crypto_df = pd.read_csv(f"{TEMP_DIR}/crypto_transformed.csv")
    crypto_df['timestamp'] = pd.to_datetime(crypto_df['timestamp'])
    crypto_df['transformed_at'] = pd.to_datetime(crypto_df['transformed_at'])
    crypto_loaded = loader.load_crypto_prices(crypto_df)
    total_loaded += crypto_loaded
    
    # Load news data
    print("  Loading news data...")
    try:
        news_df = pd.read_csv(f"{TEMP_DIR}/news_transformed.csv")
        if not news_df.empty:
            news_df['scraped_at'] = pd.to_datetime(news_df['scraped_at'])
            news_df['transformed_at'] = pd.to_datetime(news_df['transformed_at'])
            news_loaded = loader.load_news(news_df)
            total_loaded += news_loaded
        else:
            news_loaded = 0
    except Exception as e:
        print(f"  ⚠️  No news to load: {str(e)}")
        news_loaded = 0
    
    # Load portfolio data
    print("  Loading portfolio data...")
    try:
        portfolio_df = pd.read_csv(f"{TEMP_DIR}/portfolio_transformed.csv")
        if not portfolio_df.empty:
            portfolio_df['purchase_date'] = pd.to_datetime(portfolio_df['purchase_date'])
            portfolio_df['created_at'] = pd.to_datetime(portfolio_df['created_at'])
            portfolio_df['extraction_timestamp'] = pd.to_datetime(portfolio_df['extraction_timestamp'])
            portfolio_df['transformed_at'] = pd.to_datetime(portfolio_df['transformed_at'])
            portfolio_loaded = loader.load_portfolio(portfolio_df)
            total_loaded += portfolio_loaded
        else:
            portfolio_loaded = 0
    except Exception as e:
        print(f"  ⚠️  No portfolio to load: {str(e)}")
        portfolio_loaded = 0
    
    # Log pipeline metrics
    metrics = {
        'pipeline_run_id': context['dag_run'].run_id,
        'table_name': 'all_tables',
        'records_extracted': total_loaded,
        'records_transformed': total_loaded,
        'records_loaded': total_loaded,
        'errors': 0,
        'execution_time_seconds': 0,
        'status': 'SUCCESS',
        'error_message': None,
        'run_timestamp': datetime.now()
    }
    
    loader.log_pipeline_metrics(metrics)
    
    print(f"\n✅ SUCCESS: Loaded total of {total_loaded} records")
    print(f"  - Stocks: {stock_loaded}")
    print(f"  - Crypto: {crypto_loaded}")
    print(f"  - News: {news_loaded}")
    print(f"  - Portfolio: {portfolio_loaded}")
    
    context['task_instance'].xcom_push(key='total_loaded', value=total_loaded)
    context['task_instance'].xcom_push(key='stock_loaded', value=stock_loaded)
    context['task_instance'].xcom_push(key='crypto_loaded', value=crypto_loaded)
    context['task_instance'].xcom_push(key='news_loaded', value=news_loaded)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

# Task 7: Cleanup
def cleanup_temp_files():
    """Clean up temporary files"""
    import shutil
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)
    print("✅ Cleaned up temporary files")

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
)

# Task 8: Generate Summary Report
def generate_summary(**context):
    """Generate pipeline execution summary"""
    ti = context['task_instance']
    
    stock_count = ti.xcom_pull(task_ids='extract_stock_data', key='stock_count')
    crypto_count = ti.xcom_pull(task_ids='extract_crypto_data', key='crypto_count')
    news_count = ti.xcom_pull(task_ids='extract_news_data', key='news_count')
    portfolio_count = ti.xcom_pull(task_ids='extract_portfolio_data', key='portfolio_count')
    total_loaded = ti.xcom_pull(task_ids='load_to_bigquery', key='total_loaded')
    stock_symbols = ti.xcom_pull(task_ids='extract_stock_data', key='stock_symbols')
    
    # Use logical_date instead of execution_date (deprecated)
    from datetime import datetime as dt
    import pytz
    
    execution_dt = context['logical_date']
    current_dt = dt.now(pytz.UTC)
    duration = (current_dt - execution_dt).total_seconds()
    
    summary = f"""
╔══════════════════════════════════════════════════════════════╗
║         FINANCIAL MARKET ETL PIPELINE - SUCCESS             ║
╚══════════════════════════════════════════════════════════════╝

📅 Run ID: {context['dag_run'].run_id}
🕐 Execution Date: {execution_dt.strftime('%Y-%m-%d %H:%M:%S')}
⏱️  Duration: {duration:.2f} seconds

📊 EXTRACTION SUMMARY:
   • Stocks Extracted: {stock_count} records
   • Symbols: {', '.join(stock_symbols) if stock_symbols else 'N/A'}
   • Crypto Extracted: {crypto_count} records
   • News Articles: {news_count} articles
   • Portfolio Holdings: {portfolio_count} records

📤 LOADING SUMMARY:
   • Total Records Loaded: {total_loaded}
   • Destination: BigQuery (financial_data dataset)

✅ STATUS: All tasks completed successfully!

Next run scheduled: Tomorrow at 6:00 AM
    """
    
    print(summary)
    
    # Save summary to file
    with open('/opt/airflow/logs/pipeline_summary.txt', 'w') as f:
        f.write(summary)
        

summary_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary,
    dag=dag,
    email_on_success=True,
)

# Define task dependencies
setup_task  >> [extract_stocks_task, extract_crypto_task, extract_news_task, extract_portfolio_task]
[extract_stocks_task, extract_crypto_task, extract_news_task, extract_portfolio_task] >> transform_task
transform_task >> load_task >> cleanup_task >> summary_task