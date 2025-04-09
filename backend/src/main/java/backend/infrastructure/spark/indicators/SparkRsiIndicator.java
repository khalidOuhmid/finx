package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkRsiIndicator implements ISparkIndicator {
    private final int period;
    private final String priceColumn;

    public SparkRsiIndicator(int period, String priceColumn) {
        this.period = period;
        this.priceColumn = priceColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        String rsiColumn = "RSI_" + period;

        // 1. Trier les données par timestamp
        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        // 2. Calculer les différences de prix entre périodes consécutives
        WindowSpec prevWindow = Window.orderBy("timestamp");
        Dataset<Row> withPriceDiff = sortedData
                .withColumn("prev_price", functions.lag(functions.col(priceColumn), 1).over(prevWindow))
                .withColumn("price_diff", functions.col(priceColumn).minus(functions.col("prev_price")));

        // 3. Séparer les gains (variations positives) et les pertes (variations négatives)
        Dataset<Row> withGainsLosses = withPriceDiff
                .withColumn("gain", functions.when(functions.col("price_diff").gt(0), functions.col("price_diff")).otherwise(0.0))
                .withColumn("loss", functions.when(functions.col("price_diff").lt(0), functions.abs(functions.col("price_diff"))).otherwise(0.0));

        // 4. Calculer les moyennes des gains et des pertes sur la période spécifiée
        WindowSpec avgWindow = Window.orderBy("timestamp").rowsBetween(-(period-1), 0);
        Dataset<Row> withAvgs = withGainsLosses
                .withColumn("avg_gain", functions.avg(functions.col("gain")).over(avgWindow))
                .withColumn("avg_loss", functions.avg(functions.col("loss")).over(avgWindow));

        // 5. Calculer la force relative (RS = Average Gain / Average Loss)
        Dataset<Row> withRS = withAvgs
                .withColumn("rs",
                        functions.when(functions.col("avg_loss").equalTo(0), functions.lit(Double.MAX_VALUE))
                                .otherwise(functions.col("avg_gain").divide(functions.col("avg_loss")))
                );

        // 6. Appliquer la formule finale du RSI: 100 - (100 / (1 + RS))
        Dataset<Row> withRSI = withRS
                .withColumn(rsiColumn,
                        functions.when(functions.col("rs").equalTo(Double.MAX_VALUE), functions.lit(100.0))
                                .otherwise(functions.lit(100).minus(functions.lit(100).divide(functions.col("rs").plus(1))))
                );

        // 7. Nettoyer les colonnes temporaires
        return withRSI.drop("prev_price", "price_diff", "gain", "loss", "avg_gain", "avg_loss", "rs");
    }

    @Override
    public String getIndicatorType() {
        return "RSI_" + period;
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "period", period,
                "priceColumn", priceColumn
        );
    }
}
