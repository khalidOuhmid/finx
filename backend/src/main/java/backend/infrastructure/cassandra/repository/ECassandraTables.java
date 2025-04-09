package backend.infrastructure.cassandra.repository;

public enum ECassandraTables {
    STOCK_DATA,
    stock_indices,
    companies,
    companies_by_sector,
    raw_stock_data,
    technical_indicators,
    predictions,
    backtest_results,
    market_sentiment,
    user_queries
}
