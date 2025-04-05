package backend.infrastructure.spark.core.pipeline.timeseries;
import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class ValidateStructureStage implements PipelineStage {
    private final String[] requiredColumns;

    public ValidateStructureStage(String[] requiredColumns) {
        this.requiredColumns = requiredColumns;
    }


    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        for(String requiredColumn : requiredColumns) {
            if(!Arrays.asList(input.schema().fieldNames()).contains(requiredColumn)) {
                throw new IllegalArgumentException("Colonne manquante: " + requiredColumn);
            }
        }
        return input;
    }

    @Override
    public String getName() {
        return "ValidateStructureStage";
    }
}
