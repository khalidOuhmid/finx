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
        for(String requiredPath : requiredColumns) {
            try {
                input.selectExpr(requiredPath);
            } catch (Exception e) {
                throw new IllegalArgumentException("Chemin introuvable: " + requiredPath);
            }
        }
        return input;
    }

    @Override
    public String getName() {
        return "ValidateStructureStage";
    }
}
