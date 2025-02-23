-- Création du keyspace
CREATE KEYSPACE IF NOT EXISTS stock_keyspace
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
        };

USE stock_keyspace;

CREATE TABLE IF NOT EXISTS stock_indices(
                                            index_symbol text,
                                            country text,
                                            description text,
                                            company_count int,
                                            last_updated timestamp,
                                            PRIMARY KEY (index_symbol, country)
) WITH CLUSTERING ORDER BY (country ASC)
   AND comment = 'Stock market indices reference data';

CREATE TABLE IF NOT EXISTS companies (
                                         company_symbol text,
                                         company_name text,
                                         stock_index text,
                                         sector text,
                                         country text,
                                         ipo_date date,
                                         PRIMARY KEY ((company_symbol), stock_index)
) WITH CLUSTERING ORDER BY (stock_index ASC);

CREATE TABLE IF NOT EXISTS raw_stock_data(
                                             company_symbol text,
                                             time_bucket text,
                                             timestamp timestamp,
                                             open double,
                                             close double,
                                             high double,
                                             low double,
                                             volume bigint,
                                             source text,
                                             PRIMARY KEY ((company_symbol, time_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND COMPACTION = {
            'class' : 'TimeWindowCompactionStrategy',
            'compaction_window_unit' : 'DAYS',
            'compaction_window_size' : 30
            }
   AND default_time_to_live = 63072000;

ALTER TABLE raw_stock_data
    WITH bloom_filter_fp_chance = 0.01
     AND caching = {'keys' : 'ALL', 'rows_per_partition' : 'NONE'};

CREATE TYPE IF NOT EXISTS indicator_type (
                                             name text,
                                             parameters map<text, text>
                                         );

CREATE TABLE IF NOT EXISTS technical_indicators(
                                                   stock_symbol text,
                                                   indicator frozen<indicator_type>,
                                                   timestamp timestamp,
                                                   value double,
                                                   PRIMARY KEY ((stock_symbol, indicator), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND bloom_filter_fp_chance = 0.01
   AND caching = {'keys' : 'ALL', 'rows_per_partition' : '360'};

CREATE TABLE IF NOT EXISTS predictions(
                                          stock_symbol text,
                                          model_version text,
                                          prediction_time timestamp,
                                          predicted_value double,
                                          confidence_interval tuple<double, double>,
                                          features map<text, double>,
                                          PRIMARY KEY ((stock_symbol, model_version), prediction_time)
) WITH CLUSTERING ORDER BY (prediction_time DESC)
   AND gc_grace_seconds = 86400;

CREATE TABLE IF NOT EXISTS backtest_results(
                                               strategy_id uuid,
                                               backtest_date timestamp,
                                               sharpe_ratio double,
                                               max_drawdown double,
                                               total_return double,
                                               parameters map<text, text>,
                                               PRIMARY KEY (strategy_id, backtest_date)
) WITH CLUSTERING ORDER BY (backtest_date DESC)
   AND comment = 'Backtesting performance metrics';

CREATE TABLE IF NOT EXISTS user_queries(
                                           user_id uuid,
                                           query_time timestamp,
                                           query_type text,
                                           parameters map<text, text>,
                                           result_summary text,
                                           PRIMARY KEY (user_id, query_time)
) WITH CLUSTERING ORDER BY (query_time DESC)
   AND default_time_to_live = 2592000;

CREATE TABLE IF NOT EXISTS market_sentiment(
                                               symbol text,
                                               source text,
                                               timestamp timestamp,
                                               sentiment_score double,
                                               keywords map<text, int>,
                                               raw_text text,
                                               PRIMARY KEY ((symbol, source), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compression = {
            'enabled' : true,
            'class' : 'ZstdCompressor'
            };

-- Index secondaire
CREATE INDEX IF NOT EXISTS idx_company_sector
    ON companies (sector);