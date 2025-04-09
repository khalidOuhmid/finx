package backend.infrastructure.spark.config;

import org.apache.spark.SparkConf;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Getter
@Setter
@Configuration
public class SparkConfiguration {
    @Value("${spark.master.url:spark://spark-master:7077}")
    private String master;

    @Value("${spark.app.name:FinIA}")
    private String appName;

    @Value("${spark.enable.hive.support:false}")
    private boolean enableHiveSupport;

    @Value("${spark.checkpoint.dir:/tmp/spark-checkpoint}")
    private String checkpointDir;

    // Changement du sérialiseur par défaut pour éviter les problèmes avec lambdas
    @Value("${spark.serializer:org.apache.spark.serializer.JavaSerializer}")
    private String serializerClass;

    @Value("${spark.sql.shuffle.partitions:10}")
    private int shufflePartitions;

    @Value("${spark.executor.memory:2g}")
    private String executorMemory;

    @Value("${spark.cassandra.connection.host:cassandra1}")
    private String cassandraHost;

    @Value("${spark.cassandra.connection.port:9042}")
    private String cassandraPort;

    @Value("${spring.data.cassandra.keyspace-name:stock_keyspace}")
    private String cassandraKeyspace;

    @Value("${spark.driver.host:spring-app}")
    private String driverHost;

    @Value("${spark.driver.bind.address:0.0.0.0}")
    private String driverBindAddress;

    @Value("${spark.kryo.registrator:}")
    private String kryoRegistrator;

    public SparkConf buildSparkConf() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master);

        if (serializerClass != null && !serializerClass.isEmpty()) {
            conf.set("spark.serializer", serializerClass);
        } else {
            conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        }

        conf.set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                .set("spark.executor.memory", executorMemory)
                .set("spark.cassandra.connection.host", cassandraHost)
                .set("spark.cassandra.connection.port", cassandraPort)
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra")
                .set("spark.cassandra.connection.keep_alive_ms", "60000")
                .set("spark.driver.host", "spring-app")
                .set("spark.driver.bindAddress", "0.0.0.0");

        return conf;
    }


    @Profile("!docker")
    public SparkConf localSparkConf() {
        // Récupérer d'abord la configuration de base
        SparkConf conf = buildSparkConf();

        return conf.setMaster("local[*]")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.driver.host", "localhost");
    }
}
