package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StructureExtractionStage implements PipelineStage {
    private final String symbol;

    public StructureExtractionStage(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        return input.selectExpr(
                "coalesce(chart.result[0].meta.symbol, '" + symbol + "') as company_symbol",
                "chart.result[0].timestamp as timestamps",
                "chart.result[0].indicators.quote[0].open as opens",
                "chart.result[0].indicators.quote[0].close as closes",
                "chart.result[0].indicators.quote[0].high as highs",
                "chart.result[0].indicators.quote[0].low as lows",
                "chart.result[0].indicators.quote[0].volume as volumes"
        );
    }

    @Override
    public String getName() {
        return "StructureExtractionStage";
    }
}

