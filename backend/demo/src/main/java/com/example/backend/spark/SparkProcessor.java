package com.example.backend.spark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class SparkProcessor {
    private SparkSession spark;

    public SparkProcessor() {
        this.spark = SparkSession.builder()
                .appName("SparkProcessor")
                .master("local[*]")
                .getOrCreate();
    }

    public void processData(String filePath) {
        Dataset<Row> data = spark.read().json(filePath);
        data.show();
    }

    public void stop() {

        if (spark != null) {
            spark.stop();
        }
    }
}
