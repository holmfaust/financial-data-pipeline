-- migrations/setup_cleanup.sql

-- 1. Enable pg_cron
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 2. Define cleanup function
CREATE OR REPLACE FUNCTION maintain_data_retention() RETURNS void AS $$
BEGIN
    -- Delete old raw data (> 90 days)
    DELETE FROM stock_prices_raw WHERE timestamp < NOW() - INTERVAL '90 days';
    DELETE FROM crypto_prices_raw WHERE timestamp < NOW() - INTERVAL '90 days';
    
    -- Delete old metrics
    DELETE FROM stock_metrics_1min WHERE window_start < NOW() - INTERVAL '30 days';
    DELETE FROM stock_metrics_5min WHERE window_start < NOW() - INTERVAL '180 days';
    DELETE FROM price_anomalies WHERE timestamp < NOW() - INTERVAL '180 days';

    RAISE NOTICE 'Cleanup completed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- 3. Schedule daily at 3 AM
-- SELECT cron.schedule('daily-cleanup', '0 3 * * *', 'SELECT maintain_data_retention()');