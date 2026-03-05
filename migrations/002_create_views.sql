-- migrations/002_create_views.sql

-- View สำหรับคำนวณ Technical Indicators (SMA, Bollinger Bands) แบบ Real-time
-- โดยอ่านจากข้อมูลดิบ 5 นาทีที่ Spark ส่งมา
CREATE OR REPLACE VIEW view_stock_metrics_indicators AS
SELECT 
    symbol,
    window_start,
    window_end,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    volatility,
    -- คำนวณ SMA 20 (Simple Moving Average 20 periods)
    AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as calculated_sma_20,
    
    -- คำนวณ SMA 50
    AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) as calculated_sma_50,
    
    -- Bollinger Bands Upper = SMA20 + (2 * Volatility)
    (AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )) + (2 * COALESCE(volatility, 0)) as bb_upper,
    
    -- Bollinger Bands Lower = SMA20 - (2 * Volatility)
    (AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY window_start 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )) - (2 * COALESCE(volatility, 0)) as bb_lower,

    processed_at
FROM stock_metrics_5min;