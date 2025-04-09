package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkBollingerBandsIndicator implements ISparkIndicator{
    private final int period;
    private final String priceColumn;
    public SparkBollingerBandsIndicator(int period, String priceColumn ) {
        this.period = period;
        this.priceColumn = priceColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        WindowSpec windowSpec = Window.orderBy("timestamp")
                .rowsBetween(-(period-1), 0);

        Dataset<Row> result = timeSeriesData.withColumn(
                "bollinger_middle",
                functions.avg(functions.col(priceColumn)).over(windowSpec)
        );

        result = result.withColumn(
                "price_stddev",
                functions.stddev(functions.col(priceColumn)).over(windowSpec)
        );

        double multiplier = 2.0;

        return result
                .withColumn("bollinger_upper",
                        functions.col("bollinger_middle").plus(
                                functions.col("price_stddev").multiply(multiplier)))
                .withColumn("bollinger_lower",
                        functions.col("bollinger_middle").minus(
                                functions.col("price_stddev").multiply(multiplier)))
                .drop("price_stddev");
    }


    @Override
    public String getIndicatorType() {
        return "BOLLINGER_" + period;
    }

    @Override
    public Map<String, Object> getParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put("period", period);
        params.put("priceColumn", priceColumn);
        return params;
    }

}
