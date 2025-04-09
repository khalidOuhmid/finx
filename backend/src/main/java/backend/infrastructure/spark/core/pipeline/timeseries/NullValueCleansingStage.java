package backend.infrastructure.spark.core.pipeline.timeseries;
import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class NullValueCleansingStage implements PipelineStage {
    private final String[] columnsToCheck;

    public NullValueCleansingStage(String[] columnsToCheck) {
        this.columnsToCheck = columnsToCheck;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        return input.na().drop("any", columnsToCheck);
    }

    @Override
    public String getName() {
        return "NullValueCleansingStage";
    }
}
