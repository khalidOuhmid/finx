package backend.infrastructure.spark.core.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;

public class DataPipeline {
    ArrayList<PipelineStage> stages;

    public DataPipeline() {
        this.stages = new ArrayList<>();;
    }

    public DataPipeline addStage(PipelineStage stage) {
        this.stages.add(stage);
        return this;
    }

    public Dataset<Row> execute(Dataset<Row> input) {
        Dataset<Row> currentData = input;
        for (PipelineStage stage : stages) {

            System.out.println("[PIPELINE] -- Stage: " + stage.getName());
            currentData = stage.process(currentData);
        }
        return currentData;
    }
}
