package backend.infrastructure.spark.core.jobs;

import java.util.Map;

public interface ISparkJob {
    void execute(Map<String, Object> params);
}
