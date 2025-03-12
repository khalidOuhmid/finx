package backend.data.sparkdataprocessing;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SparkDataProcessor {

    private static final Logger logger = Logger.getLogger(SparkDataProcessor.class);

    @Value("${spring.cassandra.contact-points}")
    private String cassandraHost = "127.0.0.1";

    @Value("${spring.cassandra.port}")
    private int  cassandraPort  = 9042;

    @Value("${spring.cassandra.local-datacenter}")
    private String cassandraDatacenter = "datacenter1";

    private SparkSession spark;


    /**
     *Initialize the spark session with a cassandra connection
     */
    public void initializeSparkForCassandra() {
        if (spark == null || spark.sparkContext().isStopped()) {
            System.out.println("[SPARK] --- Initializing spark \n" +
                    " [SPARK] --- Cassandra Host : " + cassandraHost + "\n" +
                    " [SPARK] --- Cassandra Port : " + cassandraPort + "\n" +
                    " [SPARK] --- Cassandra Datacenter : " + cassandraDatacenter);
            try {
                spark = SparkSession.builder()
                        .appName("SparkCassandraConnector")
                        .config("spark.cassandra.connection.host", cassandraHost)
                        .config("spark.cassandra.connection.port", cassandraPort)
                        .config("spark.cassandra.datacenter", cassandraDatacenter)
                        .config("spark.cassandra.read.timeout", "10s")
                        .master("local[*]")
                        .getOrCreate();
            } catch (Exception e) {
                logger.error("[SPARK] --- Error initializing spark", e);
            }
        }
    }

    /**
     * Close the spark Session
     */
    public void closeSparkSession() {
        if (spark != null) {
            spark.close();
            System.out.println("[SPARK] --- Closing spark session");
            spark = null;
        }
    }

    /**
     * Get the spark session if initialized
     * @return the spark session
     */
    public SparkSession getSpark() {
        if (spark == null || spark.sparkContext().isStopped()) {
            throw new IllegalStateException("Spark session is not initialized");
        }
        return spark;
    }

}
