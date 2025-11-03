
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Page config
st.set_page_config(
    page_title="Financial Market Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment
load_dotenv()

# Initialize BigQuery client
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client()

client = get_bigquery_client()
project_id = os.getenv('BIGQUERY_PROJECT_ID')
dataset = 'financial_data'

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">📊 Financial Market Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.title("⚙️ Settings")
    
    # Date range selector
    date_range = st.selectbox(
        "📅 Date Range",
        ["Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time"]
    )
    
    # Symbol selector
    symbols = st.multiselect(
        "📈 Select Stocks",
        ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"],
        default=["AAPL"]
    )
    
    crypto_symbols = st.multiselect(
        "💰 Select Crypto",
        ["bitcoin", "ethereum", "cardano", "solana"],
        default=["bitcoin", "ethereum"]
    )
    
    st.markdown("---")
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 📊 Data Pipeline Status")
    st.success("✅ All systems operational")
    st.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Helper function to get date filter
def get_date_filter(range_str):
    if range_str == "Last 7 Days":
        return datetime.now() - timedelta(days=7)
    elif range_str == "Last 30 Days":
        return datetime.now() - timedelta(days=30)
    elif range_str == "Last 90 Days":
        return datetime.now() - timedelta(days=90)
    else:
        return datetime(2020, 1, 1)

# Load data functions
@st.cache_data(ttl=300)
def load_stock_data(symbols, date_filter):
    if not symbols:
        return pd.DataFrame()
    symbols_str = "', '".join(symbols)
    query = f"""
    SELECT 
        date,
        symbol,
        close,
        volume,
        daily_return,
        ma_7,
        ma_30,
        price_change_pct
    FROM `{project_id}.{dataset}.stock_prices`
    WHERE symbol IN ('{symbols_str}')
    AND date >= '{date_filter.date()}'
    ORDER BY date DESC, symbol
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_crypto_data(crypto_ids):
    if not crypto_ids:
        return pd.DataFrame()
    crypto_str = "', '".join(crypto_ids)
    query = f"""
    SELECT 
        timestamp,
        crypto_id,
        price_usd,
        market_cap,
        volume_24h,
        change_24h
    FROM `{project_id}.{dataset}.crypto_prices`
    WHERE crypto_id IN ('{crypto_str}')
    ORDER BY timestamp DESC
    LIMIT 100
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_news_data(symbols):
    if not symbols:
        return pd.DataFrame()
    symbols_str = "', '".join(symbols)
    query = f"""
    SELECT 
        symbol,
        title,
        url,
        source,
        scraped_at,
        has_bullish,
        has_bearish
    FROM `{project_id}.{dataset}.market_news`
    WHERE symbol IN ('{symbols_str}')
    ORDER BY scraped_at DESC
    LIMIT 20
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=300)
def load_portfolio_data():
    query = f"""
    SELECT 
        symbol,
        quantity,
        purchase_price,
        purchase_date,
        asset_type,
        cost_basis,
        holding_days
    FROM `{project_id}.{dataset}.user_portfolio`
    ORDER BY cost_basis DESC
    """
    try:
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame()

# Load data
date_filter = get_date_filter(date_range)

try:
    stock_df = load_stock_data(symbols, date_filter)
    crypto_df = load_crypto_data(crypto_symbols)
    news_df = load_news_data(symbols)
    portfolio_df = load_portfolio_data()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

# Main Dashboard
tab1, tab2, tab3, tab4 = st.tabs(["📈 Stock Market", "💰 Cryptocurrency", "📰 News & Sentiment", "💼 Portfolio"])

# TAB 1: Stock Market
with tab1:
    if not stock_df.empty:
        # Key Metrics Row
        st.subheader("📊 Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_change = stock_df.groupby('symbol')['price_change_pct'].mean().mean()
            st.metric(
                "Avg Price Change",
                f"{avg_change:.2f}%",
                delta=f"{avg_change:.2f}%"
            )
        
        with col2:
            total_volume = stock_df.groupby('symbol')['volume'].sum().sum()
            st.metric(
                "Total Volume",
                f"{total_volume/1e9:.2f}B"
            )
        
        with col3:
            num_stocks = len(stock_df['symbol'].unique())
            st.metric(
                "Stocks Tracked",
                num_stocks
            )
        
        with col4:
            data_points = len(stock_df)
            st.metric(
                "Data Points",
                f"{data_points:,}"
            )
        
        st.markdown("---")
        
        # Price Chart
        st.subheader("📈 Stock Price Trends")
        
        fig = go.Figure()
        
        for symbol in stock_df['symbol'].unique():
            symbol_data = stock_df[stock_df['symbol'] == symbol].sort_values('date')
            
            fig.add_trace(go.Scatter(
                x=symbol_data['date'],
                y=symbol_data['close'],
                mode='lines',
                name=symbol,
                line=dict(width=2),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                             'Date: %{x|%Y-%m-%d}<br>' +
                             'Price: $%{y:.2f}<br>' +
                             '<extra></extra>'
            ))
        
        fig.update_layout(
            title="Stock Closing Prices",
            xaxis_title="Date",
            yaxis_title="Price (USD)",
            hovermode='x unified',
            height=500,
            template="plotly_white"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Moving Averages
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Moving Averages")
            
            if len(symbols) > 0:
                selected_symbol = st.selectbox("Select stock for MA analysis", symbols)
                symbol_data = stock_df[stock_df['symbol'] == selected_symbol].sort_values('date')
                
                fig_ma = go.Figure()
                
                fig_ma.add_trace(go.Scatter(
                    x=symbol_data['date'],
                    y=symbol_data['close'],
                    name='Close Price',
                    line=dict(color='blue', width=2)
                ))
                
                fig_ma.add_trace(go.Scatter(
                    x=symbol_data['date'],
                    y=symbol_data['ma_7'],
                    name='7-Day MA',
                    line=dict(color='orange', width=1, dash='dash')
                ))
                
                fig_ma.add_trace(go.Scatter(
                    x=symbol_data['date'],
                    y=symbol_data['ma_30'],
                    name='30-Day MA',
                    line=dict(color='red', width=1, dash='dash')
                ))
                
                fig_ma.update_layout(
                    title=f"{selected_symbol} - Moving Averages",
                    xaxis_title="Date",
                    yaxis_title="Price (USD)",
                    height=400,
                    template="plotly_white"
                )
                
                st.plotly_chart(fig_ma, use_container_width=True)
        
        with col2:
            st.subheader("📊 Daily Returns Distribution")
            
            fig_dist = px.box(
                stock_df,
                x='symbol',
                y='daily_return',
                color='symbol',
                title="Daily Returns Distribution by Stock"
            )
            
            fig_dist.update_layout(
                height=400,
                template="plotly_white",
                showlegend=False
            )
            
            st.plotly_chart(fig_dist, use_container_width=True)
        
        # Volume Chart
        st.subheader("📊 Trading Volume")
        
        fig_volume = px.bar(
            stock_df.groupby(['date', 'symbol'])['volume'].sum().reset_index(),
            x='date',
            y='volume',
            color='symbol',
            title="Daily Trading Volume",
            barmode='group'
        )
        
        fig_volume.update_layout(
            height=400,
            template="plotly_white"
        )
        
        st.plotly_chart(fig_volume, use_container_width=True)
        
        # Data Table
        with st.expander("📋 View Raw Data"):
            st.dataframe(
                stock_df.sort_values('date', ascending=False),
                use_container_width=True,
                height=300
            )
    else:
        st.info("📊 Please select stocks from the sidebar to view data")

# TAB 2: Cryptocurrency
with tab2:
    if not crypto_df.empty:
        st.subheader("💰 Cryptocurrency Overview")
        
        # Latest prices
        latest_crypto = crypto_df.groupby('crypto_id').first().reset_index()
        
        cols = st.columns(len(latest_crypto))
        
        for idx, (col, row) in enumerate(zip(cols, latest_crypto.itertuples())):
            with col:
                change_color = "normal" if row.change_24h >= 0 else "inverse"
                st.metric(
                    row.crypto_id.upper(),
                    f"${row.price_usd:,.2f}",
                    f"{row.change_24h:.2f}%",
                    delta_color=change_color
                )
        
        st.markdown("---")
        
        # Price comparison
        st.subheader("💹 Price Comparison")
        
        fig_crypto = go.Figure()
        
        for crypto_id in crypto_df['crypto_id'].unique():
            crypto_data = crypto_df[crypto_df['crypto_id'] == crypto_id].sort_values('timestamp')
            
            fig_crypto.add_trace(go.Scatter(
                x=crypto_data['timestamp'],
                y=crypto_data['price_usd'],
                mode='lines+markers',
                name=crypto_id.upper(),
                line=dict(width=2)
            ))
        
        fig_crypto.update_layout(
            title="Cryptocurrency Price Trends",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            height=500,
            template="plotly_white",
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_crypto, use_container_width=True)
        
        # Market cap and volume
        col1, col2 = st.columns(2)
        
        with col1:
            fig_market_cap = px.bar(
                latest_crypto,
                x='crypto_id',
                y='market_cap',
                title="Market Capitalization",
                color='crypto_id'
            )
            fig_market_cap.update_layout(height=400, showlegend=False, template="plotly_white")
            st.plotly_chart(fig_market_cap, use_container_width=True)
        
        with col2:
            fig_volume = px.bar(
                latest_crypto,
                x='crypto_id',
                y='volume_24h',
                title="24h Trading Volume",
                color='crypto_id'
            )
            fig_volume.update_layout(height=400, showlegend=False, template="plotly_white")
            st.plotly_chart(fig_volume, use_container_width=True)
        
        # Data table
        with st.expander("📋 View Crypto Data"):
            st.dataframe(crypto_df.sort_values('timestamp', ascending=False), use_container_width=True)
    else:
        st.info("💰 Please select cryptocurrencies from the sidebar to view data")

# TAB 3: News & Sentiment
with tab3:
    if not news_df.empty:
        st.subheader("📰 Latest Market News")
        
        # Sentiment analysis
        col1, col2, col3 = st.columns(3)
        
        with col1:
            bullish_count = news_df['has_bullish'].sum()
            st.metric("🐂 Bullish Articles", int(bullish_count))
        
        with col2:
            bearish_count = news_df['has_bearish'].sum()
            st.metric("🐻 Bearish Articles", int(bearish_count))
        
        with col3:
            neutral_count = len(news_df) - bullish_count - bearish_count
            st.metric("😐 Neutral Articles", int(neutral_count))
        
        st.markdown("---")
        
        # Sentiment distribution
        sentiment_data = pd.DataFrame({
            'Sentiment': ['Bullish', 'Bearish', 'Neutral'],
            'Count': [int(bullish_count), int(bearish_count), int(neutral_count)]
        })
        
        fig_sentiment = px.pie(
            sentiment_data,
            values='Count',
            names='Sentiment',
            title="News Sentiment Distribution",
            color='Sentiment',
            color_discrete_map={'Bullish': 'green', 'Bearish': 'red', 'Neutral': 'gray'}
        )
        fig_sentiment.update_layout(height=400)
        st.plotly_chart(fig_sentiment, use_container_width=True)
        
        st.markdown("---")
        
        # News articles
        st.subheader("📰 Recent Articles")
        
        for idx, row in news_df.iterrows():
            sentiment_emoji = "🐂" if row['has_bullish'] else "🐻" if row['has_bearish'] else "😐"
            
            with st.container():
                col1, col2 = st.columns([4, 1])
                
                with col1:
                    st.markdown(f"### {sentiment_emoji} {row['title']}")
                    st.caption(f"**{row['symbol']}** | {row['source']} | {row['scraped_at'].strftime('%Y-%m-%d %H:%M')}")
                
                with col2:
                    st.markdown(f"[Read More]({row['url']})")
                
                st.markdown("---")
    else:
        st.info("📰 No news data available. Run the ETL pipeline to fetch news.")

# TAB 4: Portfolio
with tab4:
    if not portfolio_df.empty:
        st.subheader("💼 Your Portfolio")
        
        # Portfolio metrics
        total_cost = portfolio_df['cost_basis'].sum()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Holdings", len(portfolio_df))
        
        with col2:
            st.metric("Total Cost Basis", f"${total_cost:,.2f}")
        
        with col3:
            avg_holding = portfolio_df['holding_days'].mean()
            st.metric("Avg Holding Period", f"{avg_holding:.0f} days")
        
        st.markdown("---")
        
        # Portfolio allocation
        col1, col2 = st.columns(2)
        
        with col1:
            fig_allocation = px.pie(
                portfolio_df,
                values='cost_basis',
                names='symbol',
                title="Portfolio Allocation by Value"
            )
            fig_allocation.update_layout(height=400)
            st.plotly_chart(fig_allocation, use_container_width=True)
        
        with col2:
            fig_type = px.pie(
                portfolio_df,
                values='cost_basis',
                names='asset_type',
                title="Asset Type Distribution"
            )
            fig_type.update_layout(height=400)
            st.plotly_chart(fig_type, use_container_width=True)
        
        # Holdings table
        st.subheader("📊 Holdings Details")
        
        portfolio_display = portfolio_df[[
            'symbol', 'asset_type', 'quantity', 'purchase_price', 
            'cost_basis', 'purchase_date', 'holding_days'
        ]].copy()
        
        portfolio_display['purchase_price'] = portfolio_display['purchase_price'].apply(lambda x: f"${x:,.2f}")
        portfolio_display['cost_basis'] = portfolio_display['cost_basis'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(portfolio_display, use_container_width=True)
    else:
        st.info("💼 No portfolio data available. Set up the PostgreSQL database to view your portfolio.")
        
        with st.expander("📖 How to set up portfolio"):
            st.markdown("""
            1. Make sure PostgreSQL is running
            2. Run: `python scripts/setup_portfolio_db.py`
            3. Refresh this dashboard
            """)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
        <p>Financial Market Analytics Dashboard | Data Pipeline Project</p>
        <p>Built with Streamlit • BigQuery • Python</p>
    </div>
    """,
    unsafe_allow_html=True
)