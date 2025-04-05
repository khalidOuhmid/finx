package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class EmptyDatasetValidationStage implements PipelineStage {
    private final String symbol;

    public EmptyDatasetValidationStage(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        if (input.count() == 0) {
            throw new IllegalArgumentException("Aucune donnée valide après filtrage pour " + symbol);
        }
        return input;
    }

    @Override
    public String getName() {
        return "EmptyDatasetValidationStage";
    }
}

