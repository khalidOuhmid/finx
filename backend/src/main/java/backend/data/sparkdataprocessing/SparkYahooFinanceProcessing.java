package backend.data.sparkdataprocessing;
import backend.api.client.YahooFinanceService;
import backend.data.cassandra.ECassandraTables;
import jakarta.annotation.PreDestroy;
import org.apache.spark.SparkSQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class SparkYahooFinanceProcessing {

    private final SparkDataProcessor spark;
    @Value("${spring.cassandra.keyspace-name}")
    private String keyspaceName;
    /**
     * Initializes Spark session during bean creation.
     * Uses default configuration from {@link SparkDataProcessor}.
     */
    public SparkYahooFinanceProcessing() {
        this.spark = new SparkDataProcessor();
        this.spark.initializeSpark();
    }
    /**
     * Cleans up Spark resources when Spring context is destroyed.
     * Closes the Spark session gracefully.
     */
    @PreDestroy
    public void cleanup() {
        spark.closeSparkSession();
    }

    /**
     * Processes raw Yahoo Finance JSON data into a normalized Spark DataFrame.
     *
     * @param company Stock symbol (e.g., "AMZN")
     * @param range Time range for historical data (e.g., "1d", "5d")
     * @return DataFrame with normalized stock data containing:
     *         company_symbol, time_bucket, timestamp, open, close, high, low, volume, source
     * @throws SparkSQLException if data processing fails or API returns invalid format
     */
    public Dataset<Row> yahooFinanceProcessing(String company, String range) {
        try {
            String jsonData = YahooFinanceService.getGlobalQuote(company, range);
            Dataset<Row> dataFrame = spark.getSpark().read().json(jsonData);

            dataFrame = dataFrame.filter(
                    functions.size(functions.col("chart.result[0].timestamp"))
                            .equalTo(functions.size(functions.col("chart.result[0].indicators.quote[0].open"))
                            ));
            return dataFrame
                    .select(
                            functions.col("chart.result[0].meta.symbol").alias("company_symbol"),
                            functions.explode(
                                    functions.arrays_zip(
                                            functions.col("chart.result[0].timestamp"),
                                            functions.col("chart.result[0].indicators.quote[0].open"),
                                            functions.col("chart.result[0].indicators.quote[0].close"),
                                            functions.col("chart.result[0].indicators.quote[0].high"),
                                            functions.col("chart.result[0].indicators.quote[0].low"),
                                            functions.col("chart.result[0].indicators.quote[0].volume")
                                    )
                            ).alias("data")
                    )
                    .select(
                            functions.col("company_symbol"),
                            functions.from_unixtime(functions.col("data.timestamp"), "yyyy-MM").alias("time_bucket"),
                            functions.expr("CAST(data.timestamp * 1000 AS TIMESTAMP)").alias("timestamp"),
                            functions.col("data.open").cast(DataTypes.DoubleType).alias("open"),
                            functions.col("data.close").cast(DataTypes.DoubleType).alias("close"),
                            functions.col("data.high").cast(DataTypes.DoubleType).alias("high"),
                            functions.col("data.low").cast(DataTypes.DoubleType).alias("low"),
                            functions.col("data.volume").cast(DataTypes.LongType).alias("volume")
                    )
                    .withColumn("source", functions.lit("Yahoo Finance"));
        }catch (Exception e) {
            String errorMessage = "[SPARK] -- Error processing " + company + " with range " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        }

    }
    /**
     * Writes processed stock data to a Cassandra table.
     *
     * @param tableName Cassandra table enum reference
     * @param dataFrame Spark DataFrame containing normalized stock data
     * @throws SparkSQLException if Cassandra write operation fails
     */
    public void writeDataSetInCassandra(ECassandraTables tableName, Dataset<Row> dataFrame) {
        try {
            dataFrame.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", keyspaceName)
                    .option("table", tableName.toString())
                    .mode("append")
                    .save();
        }
        catch (Exception e) {
            throw new RuntimeException("[CASSANDRA] -- Write failed for table " + tableName, e);
        }
    }
    }


