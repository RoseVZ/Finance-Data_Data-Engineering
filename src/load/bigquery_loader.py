
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from datetime import datetime
import os

class BigQueryLoader:
    def __init__(self, project_id, dataset_id):
        """Initialize BigQuery client"""
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"
    
    def create_tables(self):
        """Create all necessary tables with proper schemas"""
        
        # Table 1: Stock Prices
        stock_schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT64"),
            bigquery.SchemaField("high", "FLOAT64"),
            bigquery.SchemaField("low", "FLOAT64"),
            bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("volume", "INTEGER"),
            bigquery.SchemaField("daily_return", "FLOAT64"),
            bigquery.SchemaField("price_range", "FLOAT64"),
            bigquery.SchemaField("price_change", "FLOAT64"),
            bigquery.SchemaField("price_change_pct", "FLOAT64"),
            bigquery.SchemaField("ma_7", "FLOAT64"),
            bigquery.SchemaField("ma_30", "FLOAT64"),
            bigquery.SchemaField("volatility_30d", "FLOAT64"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("transformed_at", "TIMESTAMP"),
        ]
        
        # Table 2: Crypto Prices
        crypto_schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("crypto_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price_usd", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("market_cap", "FLOAT64"),
            bigquery.SchemaField("volume_24h", "FLOAT64"),
            bigquery.SchemaField("change_24h", "FLOAT64"),
            bigquery.SchemaField("price_category", "STRING"),
            bigquery.SchemaField("transformed_at", "TIMESTAMP"),
        ]
        
        # Table 3: Market News
        news_schema = [
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("title_length", "INTEGER"),
            bigquery.SchemaField("has_earnings", "BOOLEAN"),
            bigquery.SchemaField("has_merger", "BOOLEAN"),
            bigquery.SchemaField("has_acquisition", "BOOLEAN"),
            bigquery.SchemaField("has_revenue", "BOOLEAN"),
            bigquery.SchemaField("has_profit", "BOOLEAN"),
            bigquery.SchemaField("has_loss", "BOOLEAN"),
            bigquery.SchemaField("has_growth", "BOOLEAN"),
            bigquery.SchemaField("has_decline", "BOOLEAN"),
            bigquery.SchemaField("has_bullish", "BOOLEAN"),
            bigquery.SchemaField("has_bearish", "BOOLEAN"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            bigquery.SchemaField("transformed_at", "TIMESTAMP"),
        ]
        
        # Table 4: User Portfolio
        portfolio_schema = [
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("purchase_price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("purchase_date", "DATE"),
            bigquery.SchemaField("asset_type", "STRING"),
            bigquery.SchemaField("cost_basis", "FLOAT64"),
            bigquery.SchemaField("holding_days", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("transformed_at", "TIMESTAMP"),
        ]
        
        # Table 5: Pipeline Metrics (for monitoring)
        metrics_schema = [
            bigquery.SchemaField("pipeline_run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("records_extracted", "INTEGER"),
            bigquery.SchemaField("records_transformed", "INTEGER"),
            bigquery.SchemaField("records_loaded", "INTEGER"),
            bigquery.SchemaField("errors", "INTEGER"),
            bigquery.SchemaField("execution_time_seconds", "FLOAT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        tables = {
            'stock_prices': stock_schema,
            'crypto_prices': crypto_schema,
            'market_news': news_schema,
            'user_portfolio': portfolio_schema,
            'pipeline_metrics': metrics_schema
        }
        
        for table_name, schema in tables.items():
            self._create_table(table_name, schema)
    
    def _create_table(self, table_name, schema):
        """Create a single table"""
        table_id = f"{self.dataset_ref}.{table_name}"
        
        try:
            # Check if table exists
            self.client.get_table(table_id)
            print(f"✅ Table {table_name} already exists")
        except NotFound:
            # Create table
            table = bigquery.Table(table_id, schema=schema)
            
            # Add partitioning for better performance
            if table_name == 'stock_prices':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date"
                )
            elif table_name in ['crypto_prices', 'market_news', 'pipeline_metrics']:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp" if table_name == "crypto_prices" else 
                          "scraped_at" if table_name == "market_news" else "run_timestamp"
                )
            
            table = self.client.create_table(table)
            print(f"✅ Created table {table_name}")
    
    def load_data(self, df, table_name, write_disposition="WRITE_APPEND"):
        """Load DataFrame to BigQuery table"""
        if df.empty:
            print(f"⚠️  No data to load for {table_name}")
            return 0
        
        table_id = f"{self.dataset_ref}.{table_name}"
        
        # Configure job based on write disposition
        if write_disposition == "WRITE_TRUNCATE":
            # For TRUNCATE, don't use schema update options
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=False
            )
        else:
            # For APPEND, allow schema updates
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=False,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
        
        try:
            job = self.client.load_table_from_dataframe(
                df, 
                table_id, 
                job_config=job_config
            )
            job.result()  # Wait for completion
            
            print(f"✅ Loaded {len(df)} rows to {table_name}")
            return len(df)
            
        except Exception as e:
            print(f"❌ Error loading to {table_name}: {str(e)}")
            raise
    
    def load_stock_prices(self, df):
        """Load stock prices"""
        return self.load_data(df, 'stock_prices')
    
    def load_crypto_prices(self, df):
        """Load crypto prices"""
        return self.load_data(df, 'crypto_prices')
    
    def load_news(self, df):
        """Load market news"""
        return self.load_data(df, 'market_news')
    
    def load_portfolio(self, df, write_disposition="WRITE_TRUNCATE"):
        """Load portfolio (replace existing data)"""
        return self.load_data(df, 'user_portfolio', write_disposition)
    
    def log_pipeline_metrics(self, metrics_dict):
        """Log pipeline execution metrics"""
        df = pd.DataFrame([metrics_dict])
        return self.load_data(df, 'pipeline_metrics')
    
    def query_data(self, query):
        """Execute query and return results"""
        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            return df
        except Exception as e:
            print(f"❌ Query error: {str(e)}")
            return pd.DataFrame()
    
    def get_table_row_count(self, table_name):
        """Get row count for a table"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.dataset_ref}.{table_name}`
        """
        df = self.query_data(query)
        return df['count'].iloc[0] if not df.empty else 0

# Test
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'financial_data')
    
    loader = BigQueryLoader(project_id, dataset_id)
    
    # Create all tables
    loader.create_tables()
    
    # Test loading sample data
    sample_data = pd.DataFrame({
        'date': ['2024-01-01'],
        'symbol': ['TEST'],
        'open': [100.0],
        'high': [105.0],
        'low': [99.0],
        'close': [102.0],
        'volume': [1000000],
        'daily_return': [0.02],
        'price_range': [6.0],
        'price_change': [2.0],
        'price_change_pct': [2.0],
        'ma_7': [101.0],
        'ma_30': [100.5],
        'volatility_30d': [0.015],
        'extraction_timestamp': [datetime.now()],
        'transformed_at': [datetime.now()]
    })
    
    loader.load_stock_prices(sample_data)
    
    # Check row count
    count = loader.get_table_row_count('stock_prices')
    print(f"Total rows in stock_prices: {count}")