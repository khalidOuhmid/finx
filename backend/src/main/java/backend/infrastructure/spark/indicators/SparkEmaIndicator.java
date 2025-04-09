package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkEmaIndicator implements ISparkIndicator {
private final int period;
private final double smoothingFactor;
private final String priceColumn;

    public SparkEmaIndicator(int period, double smoothingFactor, String priceColumn) {
        this.period = period;
        this.smoothingFactor = smoothingFactor;
        this.priceColumn = priceColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        String emaColumn = "EMA_" + period;

        double multiplier = smoothingFactor / (1.0 + period);

        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        WindowSpec initialWindow = Window.orderBy("timestamp")
                .rowsBetween(-(period-1), 0);
        Dataset<Row> withSMA = sortedData.withColumn(
                "SMA",
                functions.avg(functions.col(priceColumn)).over(initialWindow)
        );

        WindowSpec prevWindow = Window.orderBy("timestamp");

        Dataset<Row> result = withSMA
                .withColumn(emaColumn, functions.col("SMA"))
                .withColumn("prev_ema", functions.lag(functions.col(emaColumn), 1).over(prevWindow));

        return result
                .withColumn(emaColumn,
                        functions.when(functions.col("prev_ema").isNull(), functions.col("SMA"))
                                .otherwise(
                                        functions.col(priceColumn).multiply(multiplier)
                                                .plus(functions.col("prev_ema").multiply(1.0 - multiplier))))
                .drop("SMA", "prev_ema");
    }


    @Override
    public String getIndicatorType() {
        return "EMA" + period;
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "period", period,
                "smoothingFactor", smoothingFactor,
                "priceColumn", priceColumn
        );
    }

}
