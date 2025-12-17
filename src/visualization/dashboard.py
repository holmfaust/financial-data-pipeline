"""
Real-time Financial Dashboard using Streamlit
Displays live stock prices, charts, and analytics
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.db_manager import DatabaseManager, CacheManager

# Page config
st.set_page_config(
    page_title="Real-time Financial Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .positive {
        color: #00b894;
    }
    .negative {
        color: #d63031;
    }
</style>
""", unsafe_allow_html=True)

# Initialize managers
@st.cache_resource
def init_managers():
    return DatabaseManager(), CacheManager()

db_manager, cache_manager = init_managers()


def format_price_change(change_pct):
    """Format price change with color"""
    if change_pct is None:
        return "N/A"
    
    color = "positive" if change_pct >= 0 else "negative"
    sign = "+" if change_pct >= 0 else ""
    return f'<span class="{color}">{sign}{change_pct:.2f}%</span>'


def create_candlestick_chart(data, symbol):
    """Create candlestick chart"""
    df = pd.DataFrame(data)
    
    if df.empty:
        st.warning(f"No data available for {symbol}")
        return None
    
    fig = go.Figure(data=[go.Candlestick(
        x=df['timestamp'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price'],
        name=symbol
    )])
    
    fig.update_layout(
        title=f'{symbol} Price Chart',
        yaxis_title='Price (USD)',
        xaxis_title='Time',
        height=500,
        template='plotly_white',
        xaxis_rangeslider_visible=False
    )
    
    return fig


def create_volume_chart(data, symbol):
    """Create volume bar chart"""
    df = pd.DataFrame(data)
    
    if df.empty:
        return None
    
    fig = go.Figure(data=[go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker_color='lightblue'
    )])
    
    fig.update_layout(
        title=f'{symbol} Trading Volume',
        yaxis_title='Volume',
        xaxis_title='Time',
        height=300,
        template='plotly_white'
    )
    
    return fig


def create_line_chart(data, symbol, column, title):
    """Create line chart for metrics"""
    df = pd.DataFrame(data)
    
    if df.empty or column not in df.columns:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['window_start'],
        y=df[column],
        mode='lines',
        name=column.replace('_', ' ').title(),
        line=dict(width=2)
    ))
    
    fig.update_layout(
        title=title,
        yaxis_title=column.replace('_', ' ').title(),
        xaxis_title='Time',
        height=300,
        template='plotly_white'
    )
    
    return fig


def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<div class="main-header">📈 Real-time Financial Data Pipeline</div>', 
                unsafe_allow_html=True)
    st.markdown("### Live Market Data & Analytics")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Symbol selection
        default_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        selected_symbol = st.selectbox(
            "Select Stock Symbol",
            default_symbols,
            index=0
        )
        
        # Time range
        time_range = st.selectbox(
            "Time Range",
            ["1 Hour", "4 Hours", "1 Day", "3 Days", "7 Days"],
            index=2
        )
        
        time_map = {
            "1 Hour": 1/24,
            "4 Hours": 4/24,
            "1 Day": 1,
            "3 Days": 3,
            "7 Days": 7
        }
        days = time_map[time_range]
        
        # Refresh settings
        auto_refresh = st.checkbox("Auto Refresh", value=True)
        refresh_interval = st.slider("Refresh Interval (seconds)", 5, 60, 10)
        
        st.markdown("---")
        st.markdown("### 📊 Data Quality")
        
        # Data quality metrics
        try:
            quality_metrics = db_manager.get_data_quality_metrics()
            st.metric("Total Records", f"{quality_metrics.get('total_stock_records', 0):,}")
            
            freshness = quality_metrics.get('data_freshness_minutes', 0)
            st.metric("Data Freshness", f"{freshness:.1f} min ago")
            
            if freshness > 5:
                st.warning("⚠️ Data may be stale")
            else:
                st.success("✅ Data is fresh")
                
        except Exception as e:
            st.error(f"Error loading metrics: {e}")
    
    # Main content
    try:
        # Latest prices overview
        st.subheader("📊 Latest Prices")
        latest_prices = db_manager.get_latest_prices(default_symbols)
        
        if latest_prices:
            cols = st.columns(len(default_symbols))
            
            for idx, price_data in enumerate(latest_prices):
                with cols[idx]:
                    symbol = price_data['symbol']
                    price = float(price_data['close_price'])
                    volume = int(price_data['volume'])
                    
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{symbol}</h4>
                        <h2>${price:.2f}</h2>
                        <p>Vol: {volume:,}</p>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Detailed view for selected symbol
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader(f"📈 {selected_symbol} Price Chart")
            
            # Get historical data
            historical_data = db_manager.get_historical_data(selected_symbol, days=int(days))
            
            if historical_data:
                # Candlestick chart
                candlestick_fig = create_candlestick_chart(historical_data, selected_symbol)
                if candlestick_fig:
                    st.plotly_chart(candlestick_fig, use_container_width=True)
                
                # Volume chart
                volume_fig = create_volume_chart(historical_data, selected_symbol)
                if volume_fig:
                    st.plotly_chart(volume_fig, use_container_width=True)
            else:
                st.info(f"No historical data available for {selected_symbol}")
        
        with col2:
            st.subheader("📊 Statistics")
            
            if historical_data:
                df = pd.DataFrame(historical_data)
                
                # Calculate statistics
                current_price = df['close_price'].iloc[-1]
                high_price = df['high_price'].max()
                low_price = df['low_price'].min()
                avg_volume = df['volume'].mean()
                
                st.metric("Current Price", f"${current_price:.2f}")
                st.metric(f"{time_range} High", f"${high_price:.2f}")
                st.metric(f"{time_range} Low", f"${low_price:.2f}")
                st.metric("Avg Volume", f"{int(avg_volume):,}")
                
                # Price change
                if len(df) > 1:
                    price_change = ((current_price - df['close_price'].iloc[0]) / 
                                  df['close_price'].iloc[0] * 100)
                    st.metric("Price Change", 
                            f"{price_change:+.2f}%",
                            delta=f"${current_price - df['close_price'].iloc[0]:.2f}")
        
        # Aggregated metrics
        st.markdown("---")
        st.subheader("📉 Technical Indicators (5-min windows)")
        
        agg_metrics = db_manager.get_aggregated_metrics(selected_symbol, '5min', 50)
        
        if agg_metrics:
            col1, col2 = st.columns(2)
            
            with col1:
                # SMA chart
                sma_fig = create_line_chart(agg_metrics, selected_symbol, 
                                           'sma_20', 'Simple Moving Average (20)')
                if sma_fig:
                    st.plotly_chart(sma_fig, use_container_width=True)
            
            with col2:
                # Volatility chart
                vol_fig = create_line_chart(agg_metrics, selected_symbol,
                                           'volatility', 'Price Volatility')
                if vol_fig:
                    st.plotly_chart(vol_fig, use_container_width=True)
        
        # Anomalies detection
        st.markdown("---")
        st.subheader("🚨 Detected Anomalies")
        
        anomalies = db_manager.get_anomalies(selected_symbol, hours=24)
        
        if anomalies:
            df_anomalies = pd.DataFrame(anomalies)
            df_anomalies['timestamp'] = pd.to_datetime(df_anomalies['timestamp'])
            
            st.dataframe(
                df_anomalies[['timestamp', 'anomaly_type', 'expected_price', 
                             'actual_price', 'deviation_pct', 'severity']],
                use_container_width=True
            )
        else:
            st.success("✅ No anomalies detected in the last 24 hours")
        
    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")
        st.exception(e)
    
    # Auto refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()