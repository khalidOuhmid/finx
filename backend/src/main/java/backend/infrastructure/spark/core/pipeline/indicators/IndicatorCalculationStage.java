package backend.infrastructure.spark.core.pipeline.indicators;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import backend.infrastructure.spark.indicators.ISparkIndicator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class IndicatorCalculationStage implements PipelineStage {
    private final ISparkIndicator indicator;

    public IndicatorCalculationStage(ISparkIndicator indicator) {
        this.indicator = indicator;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        return indicator.calculate(input);
    }

    @Override
    public String getName() {
        return "";
    }
}
