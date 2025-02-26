package backend.data.sparkdataprocessing;

import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
@Component
public class SparkDataProcessor {

    @Value("${cassandra.contact-points}")
    private String cassandraHost;

    @Value("{cassandra.port}")
    private String cassandraPort;

    @Value("{cassandra.keyspace-name}")
    private String cassandraKeyspace;

    @Value("{cassandra.local-datacenter}")
    private String cassandraDatacenter;

    private SparkSession spark;

    public void initializeSpark() {
        if (spark == null || spark.sparkContext().isStopped()) {
            System.out.println("Initializing spark \n" +
                    "Cassandra Host : " + cassandraHost + "\n" +
                    "Cassandra Port : " + cassandraPort + "\n" +
                    "Cassandra Keyspace : " + cassandraKeyspace + "\n" +
                    "Cassandra Datacenter : " + cassandraDatacenter );

            spark = SparkSession.builder()
                    .appName("SparkCassandraConnector")
                    .config("spark.cassandra.connection.host",)
                    .master("local[*]")
                    .getOrCreate();
        }
    }

    public void processMarketPrice(String jsonData, String collectionName) {
        initializeSpark();
        try {
            Dataset<Row> df = spark.read()
                    .json(spark.createDataset(List.of(jsonData), Encoders.STRING()));

            Dataset<Row> transformedDF = df.selectExpr(
                    "chart.result[0].meta.symbol as symbol",
                    "chart.result[0].meta.regularMarketPrice as price",
                    "chart.result[0].indicators.quote[0].high[0] as high",
                    "chart.result[0].indicators.quote[0].low[0] as low",
                    "from_unixtime(chart.result[0].timestamp[0]) as timestamp"
            );
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String logFilePath = "logs/stock_data_" + timestamp + ".txt";

            writeToTextFile(transformedDF, logFilePath);

            transformedDF.write()
                    .format("mongodb")
                    .option("collection", collectionName)
                    .mode(SaveMode.Append)
                    .save();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeToTextFile(Dataset<Row> df, String filePath) {
        try (FileWriter fw = new FileWriter(filePath, true);
             PrintWriter pw = new PrintWriter(fw)) {

            // Écrire l'en-tête
            pw.println("Symbol,Price,High,Low,Timestamp");

            // Écrire chaque ligne de données
            List<Row> rows = df.collectAsList();
            for (Row row : rows) {
                String line = String.format("%s,%.2f,%.2f,%.2f,%s",
                        row.getAs("symbol"),
                        row.getAs("price"),
                        row.getAs("high"),
                        row.getAs("low"),
                        row.getAs("timestamp"));
                pw.println(line);
            }

            System.out.println("Données écrites dans le fichier : " + filePath);
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture dans le fichier : " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void closeSparkSession() {
        if (spark != null) {
            spark.close();
            spark = null;
        }
    }
}
