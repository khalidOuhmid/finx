package backend.infrastructure.spark.config;

import org.apache.spark.SparkConf;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SparkConfiguration {
    private String appName = "FinIA";
    private String master = "local[*]";
    private boolean enableHiveSupport = false;
    private String checkpointDir = "/tmp/spark-checkpoint";

    private String serializerClass = "org.apache.spark.serializer.KryoSerializer";
    private int shufflePartitions = 10;
    private String executorMemory = "2g";

    private String cassandraHost = "localhost";
    private String cassandraPort = "9042";
    private String cassandraKeyspace = "stock_keyspace";

    public SparkConf buildSparkConf() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.serializer", serializerClass)
                .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                .set("spark.executor.memory", executorMemory)
                .set("spark.cassandra.connection.host", cassandraHost)
                .set("spark.cassandra.connection.port", cassandraPort)
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");

        return conf;
    }
}
