package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Row;

import org.apache.spark.sql.Dataset;

import java.util.Map;

public interface ISparkIndicator {
    Dataset<Row> calculate(Dataset<Row> timeSeriesData);

    String getIndicatorType();

    Map<String,Object> getParameters();
}
