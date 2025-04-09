-- Création du keyspace avec réplication simple (single-node)
CREATE KEYSPACE IF NOT EXISTS stock_keyspace
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        } AND durable_writes = true;

USE stock_keyspace;

-- Table des indices boursiers (stock indices)
CREATE TABLE IF NOT EXISTS stock_indices (
                                             country text,
                                             index_symbol text,
                                             composition frozen<set<text>>, -- Liste des symboles composants l'indice
                                             volatility double,
                                             sector_weights map<text, double>, -- Pondération par secteur (%)
                                             last_rebalanced timestamp,
                                             PRIMARY KEY ((country, index_symbol))
) WITH compaction = {
    'class': 'LeveledCompactionStrategy',
    'sstable_size_in_mb': 1024
    }
   AND comment = 'Indices par pays avec composition dynamique';

-- Table des entreprises (company metadata)
CREATE TABLE IF NOT EXISTS companies (
                                         company_symbol text,
                                         indexed_values frozen<map<text, double>>, -- Ex : P/E ratio, Market Cap, etc.
                                         related_indicators list<text>, -- Indicateurs techniques liés (ex : RSI, SMA)
                                         PRIMARY KEY (company_symbol)
) WITH compaction = {
    'class': 'SizeTieredCompactionStrategy'
    }
   AND bloom_filter_fp_chance = 0.01;

-- Modèles factoriels (factor models)
CREATE TABLE IF NOT EXISTS factor_models (
                                             model_id uuid,
                                             factors frozen<map<text, double>>, -- Facteurs de risque et leurs pondérations
                                             r_squared double, -- Coefficient de détermination (qualité du modèle)
                                             last_updated timestamp,
                                             PRIMARY KEY (model_id)
) WITH compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 7
    };

-- Métriques de risque (risk metrics)
CREATE TABLE IF NOT EXISTS risk_metrics (
                                            portfolio_id uuid,
                                            metric_date timestamp,
                                            var_95 double, -- Value at Risk à 95%
                                            expected_shortfall double, -- Perte attendue au-delà du VaR
                                            stress_scenarios frozen<map<text, double>>, -- Résultats des tests de stress par scénario
                                            PRIMARY KEY (portfolio_id, metric_date)
) WITH CLUSTERING ORDER BY (metric_date DESC)
   AND default_time_to_live = 2592000;

-- Données de marché (market data en séries temporelles)
CREATE TABLE IF NOT EXISTS market_data_1m (
                                              symbol text,
                                              date_bucket text, -- Partition mensuelle ('YYYY-MM')
                                              timestamp timestamp,
                                              open double,
                                              close double,
                                              high double,
                                              low double,
                                              volume counter,
                                              PRIMARY KEY ((symbol, date_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_unit': 'DAYS',
            'compaction_window_size': 1
            }
   AND default_time_to_live = 2592000;

CREATE TABLE IF NOT EXISTS technical_indicators (
                                                    symbol text,
                                                    date_bucket text,
                                                    timestamp timestamp,
                                                    indicator_type text,
                                                    values map<text, double>, -- Remplace la colonne value simple
                                                    parameters map<text, double>,
                                                    PRIMARY KEY ((symbol, date_bucket, indicator_type), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_unit': 'DAYS',
            'compaction_window_size': 1
            }
   AND default_time_to_live = 2592000;

-- Corrélations entre actifs financiers (cross-asset correlations)
CREATE TABLE IF NOT EXISTS asset_correlations (
                                                  base_asset text,
                                                  target_asset text,
                                                  timeframe text, -- Ex : "daily", "weekly"
                                                  correlation double,
                                                  last_updated timestamp,
                                                  PRIMARY KEY ((base_asset), target_asset, timeframe)
) WITH compaction = {
    'class': 'SizeTieredCompactionStrategy'
    }
   AND comment = 'Corrélations historiques entre actifs';

-- Métriques de qualité des données (data quality metrics)
CREATE TABLE IF NOT EXISTS data_quality_metrics (
                                                    source text,
                                                    metric_date text, -- Format : YYYY-MM-DD
                                                    completeness double, -- % de données complètes disponibles
                                                    latency double, -- Temps moyen d'ingestion en secondes
                                                    accuracy double, -- Précision estimée (%)
                                                    PRIMARY KEY (source, metric_date)
) WITH compaction = {
    'class': 'LeveledCompactionStrategy'
    };
