> An end-to-end automated ETL pipeline for real-time financial market data analysis with Apache Airflow orchestration, Google BigQuery data warehousing, and Streamlit visualization.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-2.7.3-red.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

##  Project Overview

A production-grade data pipeline that automatically collects, processes, and visualizes financial market data from multiple sources. The system runs daily at 6 AM, processing 300+ records with automated monitoring and email notifications.

### Key Features

- ✅ **Multi-Source Data Extraction**: APIs (Alpha Vantage, CoinGecko), web scraping, PostgreSQL
- ✅ **Automated Orchestration**: Apache Airflow with scheduled daily runs
- ✅ **Cloud Data Warehouse**: Google BigQuery with partitioned tables
- ✅ **Real-Time Dashboard**: Interactive Streamlit visualization
- ✅ **Automated Monitoring**: Email notifications, retry logic, execution logs
- ✅ **Containerized Deployment**: Docker Compose for reproducible environments

##  Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                            │
├──────────────┬──────────────┬──────────────┬────────────────┤
│ Alpha Vantage│  CoinGecko   │ Web Scraping │  PostgreSQL    │
│  (Stocks)    │   (Crypto)   │   (News)     │  (Portfolio)   │
└──────┬───────┴──────┬───────┴──────┬───────┴────────┬───────┘
       │              │              │                │
       └──────────────┴──────────────┴────────────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │   APACHE AIRFLOW     │
           │    Orchestration     │
           └──────────┬───────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │   ETL PROCESSING     │
           │  • Extract           │
           │  • Transform         │
           │  • Load              │
           └──────────┬───────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │  GOOGLE BIGQUERY     │
           │   Data Warehouse     │
           └──────────┬───────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │ STREAMLIT DASHBOARD  │
           │    Visualization     │
           └──────────────────────┘
```

## Tech Stack

### Data Engineering
- **Orchestration**: Apache Airflow 2.7.3
- **Data Warehouse**: Google BigQuery
- **Database**: PostgreSQL 13
- **Containerization**: Docker, Docker Compose

### Languages & Libraries
- **Python 3.9+**: Core programming language
- **Pandas**: Data manipulation
- **Requests**: API integration
- **BeautifulSoup4**: Web scraping
- **Streamlit**: Dashboard framework
- **Plotly**: Interactive visualizations

### APIs & Data Sources
- **Alpha Vantage**: Stock market data
- **CoinGecko**: Cryptocurrency data
- **Yahoo Finance**: Financial news (web scraping)
- **Finviz**: Alternative news source

## Features Deep Dive

### Data Pipeline
- **Extraction**: Pulls data from 4 sources (APIs, web scraping, database)
- **Transformation**: 
  - Technical indicators (7-day, 30-day moving averages)
  - Volatility calculations
  - Sentiment analysis on news (bullish/bearish/neutral)
  - Data quality validation
- **Loading**: Batch uploads to BigQuery with incremental loading

### Automation
- **Scheduled Runs**: Daily at 6:00 AM UTC
- **Error Handling**: 3 automatic retries with exponential backoff
- **Notifications**: Email alerts on success/failure
- **Monitoring**: Execution logs, metrics tracking

### Dashboard
- **Stock Market Tab**: Price trends, moving averages, volume analysis
- **Cryptocurrency Tab**: Real-time prices, market cap, 24h changes
- **News & Sentiment Tab**: Latest articles with sentiment analysis
- **Portfolio Tab**: Holdings breakdown, allocation charts
