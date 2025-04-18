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


CREATE TABLE IF NOT EXISTS market_sentiment (
                                                date_bucket date,               -- jour pour le regroupement (partition efficace)
                                                timestamp timestamp,            -- horodatage précis du sentiment
                                                summary text,                   -- résumé de la situation
                                                sentiment_score double,         -- score de sentiment pour le modèle prédictif
                                                source text,                    -- source de l'information (site web, etc.)
                                                article_url text,               -- lien vers l'article original
                                                article_title text,             -- titre de l'article
                                                keywords list<text>,            -- mots-clés extraits de l'article
                                                entities list<text>,            -- entités importantes mentionnées (pays, indices, etc.)
                                                importance int,                 -- niveau d'importance (1-10)
                                                PRIMARY KEY ((date_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);


CREATE TABLE IF NOT EXISTS stock_sentiment (
                                               symbol text,                    -- symbole de l'actif (AAPL, TSLA, etc.)
                                               date_bucket date,               -- jour pour le regroupement
                                               timestamp timestamp,            -- horodatage précis
                                               summary text,                   -- résumé de la situation
                                               sentiment_score double,         -- score de sentiment
                                               evolution_24h double,           -- évolution du sentiment sur 24h
                                               evolution_7d double,            -- évolution du sentiment sur 7 jours
                                               source text,                    -- source de l'information
                                               article_url text,               -- lien vers l'article original
                                               article_title text,             -- titre de l'article
                                               keywords list<text>,            -- mots-clés pertinents
                                               mentioned_companies list<text>, -- autres entreprises mentionnées
                                               volume_impact double,           -- impact potentiel sur le volume d'échanges
                                               PRIMARY KEY ((symbol, date_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS news_articles (
                                             article_id uuid,                -- identifiant unique de l'article
                                             timestamp timestamp,            -- date de publication
                                             source text,                    -- source (investing.com, etc.)
                                             url text,                       -- URL complète
                                             title text,                     -- titre de l'article
                                             content text,                   -- contenu complet de l'article
                                             symbols list<text>,             -- symboles boursiers mentionnés
                                             processed boolean,              -- indicateur si l'article a été analysé
                                             PRIMARY KEY (article_id)
);

CREATE INDEX news_articles_by_timestamp ON news_articles (timestamp);
CREATE INDEX news_articles_by_symbols ON news_articles (symbols);


