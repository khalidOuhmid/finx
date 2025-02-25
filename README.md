# Financial Market Data Schema Documentation

## **Keyspace Configuration**
```sql
CREATE KEYSPACE IF NOT EXISTS stock_keyspace
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```
- Uses SimpleStrategy for development (switch to NetworkTopologyStrategy in production)
- Single-node configuration for testing purposes

---

## **Core Tables**

### 1. Stock Indices Reference
```sql
CREATE TABLE stock_indices (
    index_symbol text,
    country text,
    description text,
    company_count int,
    last_updated timestamp,
    PRIMARY KEY (index_symbol, country)
) WITH CLUSTERING ORDER BY (country ASC);
```
- Stores global market indices (NASDAQ, CAC40, etc.)
- Composite partition key: `(index_symbol, country)`

### 2. Company Information
```sql
CREATE TABLE companies (
    company_symbol text,
    company_name text,
    stock_index text,
    sector text,
    country text,
    ipo_date date,
    PRIMARY KEY (company_symbol, stock_index)
) WITH CLUSTERING ORDER BY (stock_index ASC);
```
- Stores company metadata with composite primary key
- Optimized for queries: `SELECT * FROM companies WHERE company_symbol = 'AAPL'`

### 3. Sector-Based Index
```sql
CREATE TABLE companies_by_sector (
    sector text,
    company_symbol text,
    company_name text,
    country text,
    PRIMARY KEY (sector, company_symbol)
);
```
- Denormalized table for sector-based queries
- Replaces secondary indexes for better performance

---

## **Market Data Storage**

### 4. Raw Market Data
```sql
CREATE TABLE raw_stock_data (
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
   AND default_time_to_live = 63072000;  -- 2-year retention
```
- Time-series optimized storage
- Uses TWCS for efficient time-based compaction
- Automatic data expiration after 2 years

### 5. Technical Indicators
```sql
CREATE TABLE technical_indicators (
    stock_symbol text,
    indicator_name text,  -- 'SMA', 'RSI', 'MACD', etc.
    period int,           -- Calculation period (e.g., 14 for RSI)
    timestamp timestamp,
    value double,
    parameters map<text, text>,  -- Additional parameters if needed
    PRIMARY KEY ((stock_symbol, indicator_name, period), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```
- Precomputed technical indicators storage
- Composite partition key allows efficient queries:
  ```sql
  SELECT * FROM technical_indicators 
  WHERE stock_symbol = 'AAPL' 
  AND indicator_name = 'SMA' 
  AND period = 50;
  ```

---

## **Machine Learning Integration**

### 6. Prediction Results
```sql
CREATE TABLE predictions (
    stock_symbol text,
    model_version text,
    prediction_time timestamp,
    predicted_value double,
    confidence_interval tuple<double, double>,
    features map<text, double>,
    PRIMARY KEY ((stock_symbol, model_version), prediction_time)
) WITH CLUSTERING ORDER BY (prediction_time DESC);
```
- Stores model predictions with versioning
- Includes feature vectors for traceability

### 7. Backtest Metrics
```sql
CREATE TABLE backtest_results (
    strategy_id uuid,
    backtest_date timestamp,
    sharpe_ratio double,
    max_drawdown double,
    total_return double,
    parameters map<text, text>,
    PRIMARY KEY (strategy_id, backtest_date)
) WITH CLUSTERING ORDER BY (backtest_date DESC);
```
- Historical backtesting performance tracking

---

## **Sentiment Analysis**

### 8. Social Sentiment
```sql
CREATE TABLE market_sentiment (
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
   };
```
- Stores social media sentiment data
- Zstd compression reduces storage footprint by ~80%

---

## **Audit & Monitoring**

### 9. User Queries Log
```sql
CREATE TABLE user_queries (
    user_id uuid,
    query_time timestamp,
    query_type text,
    parameters map<text, text>,
    result_summary text,
    PRIMARY KEY (user_id, query_time)
) WITH CLUSTERING ORDER BY (query_time DESC)
   AND default_time_to_live = 2592000;  -- 30-day retention
```
- Automatically expires audit logs after 30 days

