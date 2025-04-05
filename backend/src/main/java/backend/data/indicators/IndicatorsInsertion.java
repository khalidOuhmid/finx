package backend.data.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class IndicatorsInsertion {
    protected final SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(IndicatorsInsertion.class);

    public IndicatorsInsertion(SparkSession spark) {
        this.spark = spark;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public Dataset<Row> processDataToIndicators(Dataset<Row> indicators) {

    }
    public boolean validateIndicatorsStructure (Dataset<Row> indicators) {}
    public Dataset<Row> createIndicatorsEmptyDataFrame(String company) {
        return getSpark().createDataFrame(
                Collections.emptyList(),
                new StructType()
                        .add("symbol", DataTypes.StringType)
                        .add("date_bucket", DataTypes.StringType)
                        .add("indicator_type", DataTypes.StringType)
                        .add("value", DataTypes.DoubleType)
                        .add("parameters", DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType))
        );
    }


    public Dataset<Row> getMarketData(String companySymbol, String dateBucket) {
        try {
            getSpark().conf().set("spark.sql.catalog.cassandra",
                    "com.datastax.spark.connector.datasource.CassandraCatalog");

            return getSpark().read()
                    .format("org.apache.spark.sql.cassandra")
                    .options(Map.of(
                            "table", "market_data_1m",
                            "keyspace", "stock_keyspace"))
                    .load()
                    .filter(functions.col("symbol").equalTo(companySymbol)
                            .and(functions.col("date_bucket").equalTo(dateBucket)));
        }
        catch (Exception e) {
            logger.error("Erreur lors de la récupération des données de marché: {}", e.getMessage(), e);
            return getSpark().createDataFrame(
                    Collections.emptyList(),
                    new StructType()
                            .add("symbol", DataTypes.StringType)
                            .add("date_bucket", DataTypes.StringType)
                            .add("timestamp", DataTypes.TimestampType)
                            .add("open", DataTypes.DoubleType)
                            .add("close", DataTypes.DoubleType)
                            .add("high", DataTypes.DoubleType)
                            .add("low", DataTypes.DoubleType)
                            .add("volume", DataTypes.LongType)
            );
        }
    }
}
