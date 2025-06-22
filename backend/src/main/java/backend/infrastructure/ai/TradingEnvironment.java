package backend.infrastructure.ai;

import backend.infrastructure.spark.config.SparkSessionProvider;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.*;

public class TradingEnvironment {
    public static final int ACTION_SELL = 0;
    public static final int ACTION_HOLD = 1;
    public static final int ACTION_BUY = 2;
    private final SparkSessionProvider sparkProvider;
    private final SparkSession spark;
    private String currentSymbol;
    private String currentDateBucket;
    private int currentStep = 0;
    private Dataset<Row> marketData;
    private Dataset<Row> technicalIndicators;
    private Dataset<Row> sentimentData;

    public TradingEnvironment(SparkSessionProvider sparkProvider) {
        this.sparkProvider = sparkProvider;
        this.spark = sparkProvider.session();
    }

    public Dataset<Row> reset(String symbol, String dateBucket) {
        this.currentSymbol = symbol;
        this.currentDateBucket = dateBucket;
        this.currentStep = 0;

        this.marketData = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "stock_keyspace")
                .option("table", "market_data_1m")
                .load()
                .filter(col("symbol").equalTo(symbol))
                .filter(col("date_bucket").equalTo(dateBucket))
                .orderBy("timestamp");

        this.technicalIndicators = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "stock_keyspace")
                .option("table", "technical_indicators")
                .load()
                .filter(col("symbol").equalTo(symbol))
                .filter(col("date_bucket").equalTo(dateBucket))
                .orderBy("timestamp");
        return getState();
    }

    public Dataset<Row> getState() {
        // 1. Obtenir les données de marché actuelles jusqu'au pas courant
        Dataset<Row> currentMarketData = this.marketData
                .orderBy("timestamp")
                .limit(this.currentStep + 1);

        // 2. Récupérer la dernière ligne (état actuel)
        Row lastRow = currentMarketData.orderBy(col("timestamp").desc()).first();
        if (lastRow == null) {
            throw new IllegalStateException("Pas de données disponibles pour l'état actuel");
        }

        // 3. Obtenir le timestamp actuel pour les jointures
        java.sql.Timestamp currentTimestamp = lastRow.getTimestamp(lastRow.fieldIndex("timestamp"));

        // 4. Calculer des caractéristiques de marché sur la fenêtre courante
        Dataset<Row> marketFeatures = currentMarketData.agg(
                max("high").as("max_price"),
                min("low").as("min_price"),
                avg("close").as("avg_price"),
                stddev("close").as("volatility"),
                sum("volume").as("total_volume"),
                last("close").as("current_price")
        );

        // 5. Récupérer les derniers indicateurs techniques disponibles
        Dataset<Row> currentIndicators = this.technicalIndicators
                .filter(col("timestamp").leq(currentTimestamp))
                .orderBy(col("timestamp").desc())
                .limit(1);

        // 6. Récupérer le sentiment du marché si disponible
        Dataset<Row> currentSentiment = null;
        try {
            currentSentiment = spark.read()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "stock_keyspace")
                    .option("table", "stock_sentiment")
                    .load()
                    .filter(col("symbol").equalTo(currentSymbol))
                    .filter(col("date_bucket").equalTo(currentDateBucket.substring(0, 10))) // Extraction de la date
                    .filter(col("timestamp").leq(currentTimestamp))
                    .orderBy(col("timestamp").desc())
                    .limit(1)
                    .select("sentiment_score", "evolution_24h", "volume_impact");
        } catch (Exception e) {
            // Si le sentiment n'est pas disponible, créer un DataFrame vide avec structure appropriée
            currentSentiment = spark.createDataFrame(
                    spark.emptyDataFrame().javaRDD(),
                    spark.read().format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "stock_keyspace")
                            .option("table", "stock_sentiment")
                            .load()
                            .limit(0)
                            .select("sentiment_score", "evolution_24h", "volume_impact")
                            .schema()
            );
        }

        // 7. Fusionner toutes les caractéristiques (cross join car un seul enregistrement par dataset)
        Dataset<Row> combinedFeatures = marketFeatures.crossJoin(currentIndicators);
        if (currentSentiment != null && !currentSentiment.isEmpty()) {
            combinedFeatures = combinedFeatures.crossJoin(currentSentiment);
        }

        return combinedFeatures;
    }


    public StepResult  step(int action) {
        Dataset<Row> currentState = getState();
        Row currentStateRow = currentState.first();
        double currentPrice = currentStateRow.getDouble(currentStateRow.fieldIndex("current_price"));
        this.currentStep++;
        boolean done = this.currentStep >= this.marketData.count() - 1;
        Dataset<Row> nextState = getState();
        // 5. Calculer la récompense basée sur l'action et le changement de prix
        double reward = 0.0;
        if (!done && nextState != null && !nextState.isEmpty()) {
            Row nextStateRow = nextState.first();
            double nextPrice = nextStateRow.getDouble(nextStateRow.fieldIndex("current_price"));
            reward = calculateReward(currentPrice, nextPrice, action);
        }

        return new StepResult(nextState, reward, done);
    }

    private double calculateReward(double previousPrice, double currentPrice, int action) {
        // action: 0 = vendre, 1 = conserver, 2 = acheter
        double priceChange = (currentPrice - previousPrice) / previousPrice;

        switch (action) {
            case ACTION_SELL: // Vendre
                return -priceChange; // Positif si le prix baisse
            case ACTION_BUY: // Acheter
                return priceChange;  // Positif si le prix monte
            case ACTION_HOLD: // Conserver
            default:
                return Math.abs(priceChange) < 0.001 ? 0.001 : 0; // Petite récompense pour stabilité
        }

    }
}
