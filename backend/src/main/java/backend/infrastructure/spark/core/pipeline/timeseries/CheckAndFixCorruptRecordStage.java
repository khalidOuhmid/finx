package backend.infrastructure.spark.core.pipeline.timeseries;

import backend.infrastructure.spark.core.pipeline.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class CheckAndFixCorruptRecordStage implements PipelineStage {

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        if (Arrays.asList(input.columns()).contains("_corrupt_record")) {
            long corruptCount = input.filter(functions.col("_corrupt_record").isNotNull()).count();
            long totalCount = input.count();

            System.err.println("Enregistrements corrompus trouvés "
                    + corruptCount + "/" + totalCount + " ("
                    + (totalCount > 0 ? (corruptCount * 100.0 / totalCount) : 0) + "%)");

            input.select("_corrupt_record").filter(functions.col("_corrupt_record").isNotNull())
                    .show(5, false);

            if (input.columns().length == 1) {
                throw new IllegalArgumentException("JSON entièrement corrompu");
            }

            return input.filter(functions.col("_corrupt_record").isNull());
        }
        return input;
    }

    @Override
    public String getName() {
        return "CheckAndFixCorruptRecordStage";
    }
}
