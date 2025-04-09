package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

public class SparkMovingAverageIndicator implements ISparkIndicator {
    private final int period;
    private final String priceColumn;

    public SparkMovingAverageIndicator(int period, String priceColumn) {
        this.period = period;
        this.priceColumn = priceColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        WindowSpec windowSpec = Window.orderBy("timestamp")
            .rowsBetween(-(period-1),0);
        return timeSeriesData.withColumn(
                "SMA_" + period,
                functions.avg(timeSeriesData.col(priceColumn)).over(windowSpec)
        );
    }

    @Override
    public String getIndicatorType() {
        return "SMA_" + period;
    }

    @Override
    public Map<String, Object> getParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put("period", period);
        params.put("priceColumn", priceColumn);
        return params;
    }
}
