
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime, timedelta
import random

class PortfolioDatabase:
    def __init__(self, config):
        self.config = config
        self.conn = None
    
    def connect(self):
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            print("✅ Connected to PostgreSQL")
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
    
    def create_tables(self):
        """Create portfolio tables"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_portfolio (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            symbol VARCHAR(10),
            quantity DECIMAL(18, 8),
            purchase_price DECIMAL(18, 2),
            purchase_date DATE,
            asset_type VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            symbol VARCHAR(10),
            transaction_type VARCHAR(10),
            quantity DECIMAL(18, 8),
            price DECIMAL(18, 2),
            transaction_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            self.conn.commit()
            cursor.close()
            print("✅ Tables created successfully")
        except Exception as e:
            print(f"❌ Error creating tables: {str(e)}")
    
    def insert_sample_data(self):
        """Insert sample portfolio data"""
        sample_holdings = [
            (1, 'AAPL', 10, 150.00, '2024-01-15', 'stock'),
            (1, 'GOOGL', 5, 140.00, '2024-02-01', 'stock'),
            (1, 'MSFT', 8, 380.00, '2024-01-20', 'stock'),
            (1, 'bitcoin', 0.5, 45000.00, '2024-01-10', 'crypto'),
            (1, 'ethereum', 2, 2500.00, '2024-02-05', 'crypto'),
        ]
        
        insert_query = """
        INSERT INTO user_portfolio (user_id, symbol, quantity, purchase_price, purchase_date, asset_type)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(insert_query, sample_holdings)
            self.conn.commit()
            cursor.close()
            print(f"✅ Inserted {len(sample_holdings)} sample holdings")
        except Exception as e:
            print(f"❌ Error inserting data: {str(e)}")
    
    def extract_portfolio_data(self):
        """Extract portfolio data"""
        query = """
        SELECT 
            user_id,
            symbol,
            quantity,
            purchase_price,
            purchase_date,
            asset_type,
            created_at
        FROM user_portfolio
        WHERE user_id = 1
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            df['extraction_timestamp'] = datetime.now()
            print(f"✅ Extracted {len(df)} portfolio records")
            return df
        except Exception as e:
            print(f"❌ Error extracting portfolio: {str(e)}")
            return pd.DataFrame()
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()

# Test
if __name__ == "__main__":
    config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'portfolio_db',
        'user': 'postgres',
        'password': 'yourpassword'
    }
    
    db = PortfolioDatabase(config)
    db.connect()
    db.create_tables()
    db.insert_sample_data()
    df = db.extract_portfolio_data()
    print(df)
    db.close()