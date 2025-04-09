package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkMacdIndicator implements ISparkIndicator {
    private final int fastPeriod;
    private final int slowPeriod;
    private final int signalPeriod;
    private final double smoothingFactor;
    private final String priceColumn;

    public SparkMacdIndicator(int fastPeriod, int slowPeriod, int signalPeriod, double smoothingFactor, String priceColumn) {
        this.fastPeriod = fastPeriod;
        this.slowPeriod = slowPeriod;
        this.signalPeriod = signalPeriod;
        this.smoothingFactor = smoothingFactor;
        this.priceColumn = priceColumn;
    }

    public SparkMacdIndicator(String priceColumn) {
        this(12, 26, 9, 2.0, priceColumn);
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        // 2. Calculer l'EMA rapide (EMA-12)
        String fastEmaCol = "EMA_" + fastPeriod;
        sortedData = calculateEma(sortedData, fastPeriod, smoothingFactor, priceColumn, fastEmaCol);

        // 3. Calculer l'EMA lente (EMA-26)
        String slowEmaCol = "EMA_" + slowPeriod;
        sortedData = calculateEma(sortedData, slowPeriod, smoothingFactor, priceColumn, slowEmaCol);

        // 4. Calculer la ligne MACD (différence entre EMAs)
        String macdCol = "MACD";
        Dataset<Row> withMacd = sortedData.withColumn(
                macdCol,
                functions.col(fastEmaCol).minus(functions.col(slowEmaCol))
        );

        // 5. Calculer la ligne de signal (EMA de la ligne MACD)
        String signalCol = "MACD_Signal";
        Dataset<Row> withSignal = calculateEma(withMacd, signalPeriod, smoothingFactor, macdCol, signalCol);

        // 6. Calculer l'histogramme (MACD - Signal)
        String histogramCol = "MACD_Histogram";
        Dataset<Row> result = withSignal.withColumn(
                histogramCol,
                functions.col(macdCol).minus(functions.col(signalCol))
        );

        return result;
    }

    /**
     * Méthode utilitaire pour calculer l'EMA
     */
    private Dataset<Row> calculateEma(Dataset<Row> data, int period, double smoothingFactor, String inputColumn, String outputColumn) {
        double multiplier = smoothingFactor / (1.0 + period);

        // Créer une fenêtre pour le calcul initial (SMA)
        WindowSpec initialWindow = Window.orderBy("timestamp")
                .rowsBetween(-(period-1), 0);
        Dataset<Row> withSMA = data.withColumn(
                "SMA_temp",
                functions.avg(functions.col(inputColumn)).over(initialWindow)
        );

        // Créer une fenêtre pour accéder aux valeurs précédentes
        WindowSpec prevWindow = Window.orderBy("timestamp");

        // Initialiser l'EMA et référencer l'EMA précédente
        Dataset<Row> withEma = withSMA
                .withColumn(outputColumn, functions.col("SMA_temp"))
                .withColumn("prev_ema", functions.lag(functions.col(outputColumn), 1).over(prevWindow));

        // Appliquer la formule de l'EMA
        return withEma
                .withColumn(outputColumn,
                        functions.when(functions.col("prev_ema").isNull(), functions.col("SMA_temp"))
                                .otherwise(
                                        functions.col(inputColumn).multiply(multiplier)
                                                .plus(functions.col("prev_ema").multiply(1.0 - multiplier))))
                .drop("SMA_temp", "prev_ema");
    }

    @Override
    public String getIndicatorType() {
        return "MACD";
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "fastPeriod", fastPeriod,
                "slowPeriod", slowPeriod,
                "signalPeriod", signalPeriod,
                "smoothingFactor", smoothingFactor,
                "priceColumn", priceColumn
        );
    }
}
