package backend.infrastructure.spark.config;

import org.apache.spark.sql.SparkSession;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkSessionProvider {
    private final SparkConfiguration config;
    private static SparkSession sparkSession;

    public SparkSessionProvider(SparkConfiguration config) {
        this.config = config;
    }

    public SparkSession session() {
        if (sparkSession == null) {
            synchronized (SparkSessionProvider.class) {
                if (sparkSession == null) {
                    log.info("Initializing Spark session with configuration: {}", config);
                    SparkSession.Builder builder = SparkSession.builder()
                            .config(config.buildSparkConf());

                    if (config.isEnableHiveSupport()) {
                        builder.enableHiveSupport();
                    }

                    sparkSession = builder.getOrCreate();
                    sparkSession.sparkContext().setCheckpointDir(config.getCheckpointDir());

                    log.info("Spark session initialized successfully");
                }
            }
        }
        return sparkSession;
    }

    public void closeSession() {
        if (sparkSession != null) {
            sparkSession.close();
            sparkSession = null;
            log.info("Spark session closed");
        }
    }
}
