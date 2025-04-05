package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ArraySizeValidationStage implements PipelineStage {

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        return input.filter(
                functions.size(functions.col("timestamps")).gt(0)
                        .and(functions.size(functions.col("opens")).gt(0))
                        .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("opens"))))
                        .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("closes"))))
                        .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("highs"))))
                        .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("lows"))))
                        .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("volumes"))))
        );
    }

    @Override
    public String getName() {
        return "ArraySizeValidationStage";
    }
}
