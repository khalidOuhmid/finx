package backend.data.dataprocessing.json;

import backend.api.client.YahooFinanceService;
import backend.data.cassandra.ECassandraTables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

public abstract class AbstractYahooFinanceProcessor implements IYahooFinanceProcessor {
    protected final SparkSession spark;

    public AbstractYahooFinanceProcessor(SparkSession spark) {
        this.spark = spark;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public String getYahooFinanceJson(String symbol, String range, String interval){
        String jsonData = YahooFinanceService.getGlobalQuote(symbol, range, interval);
        if (jsonData == null || jsonData.isEmpty()) {
            System.err.println("Données JSON vides ou nulles pour " + symbol);
            return "";
        }
        return jsonData;
    }

    @Override
    public StructType getYahooFinanceSchema() {
        return new StructType()
                .add("chart", new StructType()
                        .add("result", new ArrayType(
                                new StructType()
                                        .add("meta", new StructType()
                                                .add("symbol", DataTypes.StringType)
                                                .add("currency", DataTypes.StringType)
                                                .add("exchangeName", DataTypes.StringType)
                                                .add("regularMarketPrice", DataTypes.DoubleType)
                                                .add("previousClose", DataTypes.DoubleType)
                                                .add("dataGranularity", DataTypes.StringType)
                                                .add("range", DataTypes.StringType))
                                        .add("timestamp", new ArrayType(DataTypes.LongType, true))
                                        .add("indicators", new StructType()
                                                .add("quote", new ArrayType(
                                                        new StructType()
                                                                .add("open", new ArrayType(DataTypes.DoubleType, true))
                                                                .add("close", new ArrayType(DataTypes.DoubleType, true))
                                                                .add("high", new ArrayType(DataTypes.DoubleType, true))
                                                                .add("low", new ArrayType(DataTypes.DoubleType, true))
                                                                .add("volume", new ArrayType(DataTypes.LongType, true)),
                                                        true))),
                                true))
                        .add("error", DataTypes.StringType));
    }

    @Override
    public Dataset<Row> createEmptyDataFrame(String company) {
        return getSpark().createDataFrame(
                Collections.emptyList(),
                new StructType()
                        .add("company_symbol", DataTypes.StringType)
                        .add("time_bucket", DataTypes.StringType)
                        .add("timestamp", DataTypes.TimestampType)
                        .add("open", DataTypes.DoubleType)
                        .add("close", DataTypes.DoubleType)
                        .add("high", DataTypes.DoubleType)
                        .add("low", DataTypes.DoubleType)
                        .add("volume", DataTypes.LongType)
                        .add("source", DataTypes.StringType)
        );
    }

    // Ces méthodes doivent être implémentées par les classes concrètes
    @Override
    public abstract Dataset<Row> yahooFinanceProcessing(String jsonData, String company);

    @Override
    public abstract boolean writeDataSetInCassandra(ECassandraTables tableName, Dataset<Row> dataFrame);

    @Override
    public abstract boolean validateYahooFinanceStructure(Dataset<Row> dataFrame);
}
