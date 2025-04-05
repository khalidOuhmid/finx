package backend.infrastructure.spark.core.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface PipelineStage {
    Dataset<Row> process(Dataset<Row> input);
    String getName();
}

