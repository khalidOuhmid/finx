package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DropDuplicatesStage implements PipelineStage {
    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        return input.dropDuplicates("timestamp");
    }

    @Override
    public String getName() {
        return "DropDuplicatesStage";
    }
}
