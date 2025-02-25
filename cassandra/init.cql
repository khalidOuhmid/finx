CREATE KEYSPACE IF NOT EXISTS stock_keyspace
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        };

USE stock_keyspace;

-- Table for stock market indices (NASDAQ, CAC40, etc.)
CREATE TABLE IF NOT EXISTS stock_indices (
                                             index_symbol text,
                                             country text,
                                             description text,
                                             company_count int,
                                             last_updated timestamp,
                                             PRIMARY KEY (index_symbol, country)
) WITH CLUSTERING ORDER BY (country ASC)
   AND comment = 'Reference data for global stock market indices';

-- Company information table
CREATE TABLE IF NOT EXISTS companies (
                                         company_symbol text,
                                         company_name text,
                                         stock_index text,
                                         sector text,
                                         country text,
                                         ipo_date date,
                                         PRIMARY KEY (company_symbol, stock_index)
) WITH CLUSTERING ORDER BY (stock_index ASC)
   AND comment = 'Company metadata and index membership';

-- Reverse index for sector-based queries
CREATE TABLE IF NOT EXISTS companies_by_sector (
                                                   sector text,
                                                   company_symbol text,
                                                   company_name text,
                                                   country text,
                                                   PRIMARY KEY (sector, company_symbol)
) WITH CLUSTERING ORDER BY (company_symbol ASC)
   AND comment = 'Denormalized table for sector-based lookups';

-- Raw market data storage (time-series optimized)
CREATE TABLE IF NOT EXISTS raw_stock_data (
                                              company_symbol text,
                                              time_bucket text,  -- Format: 'YYYY-MM' for monthly partitioning
                                              timestamp timestamp,
                                              open double,
                                              close double,
                                              high double,
                                              low double,
                                              volume bigint,
                                              source text,
                                              PRIMARY KEY ((company_symbol, time_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_unit': 'DAYS',
            'compaction_window_size': 30
            }
   AND default_time_to_live = 63072000  -- 2-year retention
   AND comment = 'Raw price/volume data with automatic expiration';

-- Precomputed technical indicators storage
CREATE TABLE IF NOT EXISTS technical_indicators (
                                                    stock_symbol text,
                                                    indicator_name text,  -- 'SMA', 'RSI', 'MACD', etc.
                                                    period int,           -- Calculation period (e.g., 14 for RSI)
                                                    timestamp timestamp,
                                                    value double,
                                                    parameters map<text, text>,  -- Additional parameters if needed
                                                    PRIMARY KEY ((stock_symbol, indicator_name, period), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND caching = {
            'keys': 'ALL',
            'rows_per_partition': '360'
            }
   AND comment = 'Precomputed technical analysis indicators';

-- Machine learning predictions storage
CREATE TABLE IF NOT EXISTS predictions (
                                           stock_symbol text,
                                           model_version text,
                                           prediction_time timestamp,
                                           predicted_value double,
                                           confidence_interval tuple<double, double>,
                                           features map<text, double>,
                                           PRIMARY KEY ((stock_symbol, model_version), prediction_time)
) WITH CLUSTERING ORDER BY (prediction_time DESC)
   AND comment = 'AI/ML model prediction results';

-- Backtesting results storage
CREATE TABLE IF NOT EXISTS backtest_results (
                                                strategy_id uuid,
                                                backtest_date timestamp,
                                                sharpe_ratio double,
                                                max_drawdown double,
                                                total_return double,
                                                parameters map<text, text>,
                                                PRIMARY KEY (strategy_id, backtest_date)
) WITH CLUSTERING ORDER BY (backtest_date DESC)
   AND comment = 'Historical backtesting performance metrics';

-- Social media sentiment data
CREATE TABLE IF NOT EXISTS market_sentiment (
                                                symbol text,
                                                source text,
                                                timestamp timestamp,
                                                sentiment_score double,
                                                keywords map<text, int>,
                                                raw_text text,
                                                PRIMARY KEY ((symbol, source), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compression = {
            'sstable_compression': 'ZstdCompressor',
            'chunk_length_kb': 64
            }
   AND comment = 'Sentiment analysis from social media sources';

-- User query audit log
CREATE TABLE IF NOT EXISTS user_queries (
                                            user_id uuid,
                                            query_time timestamp,
                                            query_type text,
                                            parameters map<text, text>,
                                            result_summary text,
                                            PRIMARY KEY (user_id, query_time)
) WITH CLUSTERING ORDER BY (query_time DESC)
   AND default_time_to_live = 2592000  -- 30-day retention
   AND comment = 'User activity and query audit log';