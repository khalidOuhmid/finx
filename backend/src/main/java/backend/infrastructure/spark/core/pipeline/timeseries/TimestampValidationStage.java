package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class TimestampValidationStage implements PipelineStage {
    private final String symbol;

    public TimestampValidationStage(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        if (input.select("timestamps").filter(functions.col("timestamps").isNotNull()).limit(1).count() == 0) {
            throw new IllegalArgumentException("Aucune donnée temporelle trouvée pour " + symbol);
        }
        return input;
    }

    @Override
    public String getName() {
        return "TimestampValidationStage";
    }
}

