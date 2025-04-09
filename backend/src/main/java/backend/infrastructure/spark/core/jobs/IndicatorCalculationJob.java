package backend.infrastructure.spark.core.jobs;

import backend.infrastructure.spark.config.SparkSessionProvider;
import backend.infrastructure.spark.core.pipeline.DataPipeline;
import backend.infrastructure.spark.core.pipeline.indicators.IndicatorCalculationStage;
import backend.infrastructure.spark.indicators.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class IndicatorCalculationJob extends AbstractSparkJob {
    private static final List<String> INDICATOR_PREFIXES =
            Arrays.asList("SMA_", "bollinger_", "RSI_", "MACD_", "stochastic_", "OBV_");

    public IndicatorCalculationJob(SparkSessionProvider sparkProvider) {
        super(sparkProvider);
    }

    @Override
    public void execute(Map<String, Object> params) {
        try {
            String symbol = (String) params.get("symbol");
            String dateBucket = (String) params.get("dateBucket");

            Dataset<Row> timeSeriesData = loadTimeSeriesData(symbol, dateBucket);

            List<ISparkIndicator> indicators = configureIndicators();

            DataPipeline pipeline = buildPipeline(indicators);
            Dataset<Row> enrichedData = pipeline.execute(timeSeriesData);

            saveIndicators(enrichedData, symbol, dateBucket);

        } catch (Exception e) {
            log.error("Erreur lors du calcul des indicateurs: {}", e.getMessage(), e);
        }
    }


    private Dataset<Row> loadTimeSeriesData(String symbol, String dateBucket) {
        return spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "stock_keyspace")
                .option("table", "market_data_1m")
                .load()
                .filter(col("symbol").equalTo(symbol))
                .filter(col("date_bucket").equalTo(dateBucket))
                .orderBy("timestamp");
    }

    private List<ISparkIndicator> configureIndicators() {
        List<ISparkIndicator> indicators = new ArrayList<>();

        indicators.add(new SparkMovingAverageIndicator(20, "close"));
        indicators.add(new SparkBollingerBandsIndicator(20, "close"));
        indicators.add(new SparkRsiIndicator(14, "close"));
        indicators.add(new SparkMacdIndicator(12, 26, 9, 2.0, "close"));
        indicators.add(new SparkEmaIndicator(12, 2.0, "close"));
        indicators.add(new SparkStochasticOscillatorIndicator(14, 3, "close", "high", "low"));
        indicators.add(new SparkOnBalanceVolumeIndicator("close", "volume"));

        return indicators;
    }

    private DataPipeline buildPipeline(List<ISparkIndicator> indicators) {
        DataPipeline pipeline = new DataPipeline();

        for (ISparkIndicator indicator : indicators) {
            pipeline.addStage(new IndicatorCalculationStage(indicator));
        }

        return pipeline;
    }

    private void saveIndicators(Dataset<Row> data, String symbol, String dateBucket) {
        // 1. Obtenir tous les noms de colonnes qui sont des indicateurs
        List<String> indicatorColumns = new ArrayList<>();
        for (String colName : data.columns()) {
            if (isIndicatorColumn(colName)) {
                indicatorColumns.add(colName);
            }
        }

        Column[] indicatorNameLiterals = indicatorColumns.stream()
                .map(functions::lit)
                .toArray(Column[]::new);

        Column[] indicatorValueColumns = indicatorColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);

        Dataset<Row> pivotedData = data.select(
                        col("timestamp"),
                        explode(
                                arrays_zip(
                                        array(indicatorNameLiterals),
                                        array(indicatorValueColumns)
                                )
                        ).as("zipped")
                )
                .select(
                        col("timestamp"),
                        col("zipped._1").as("indicator_name"),
                        col("zipped._2").as("indicator_value")
                );

        Dataset<Row> groupedIndicators = pivotedData
                .withColumn("indicator_type", extractIndicatorType(col("indicator_name")))
                .withColumn("value_type", extractValueType(col("indicator_name")))
                .groupBy("timestamp", "indicator_type")
                .agg(
                        map_from_entries(
                                collect_list(
                                        struct(col("value_type"), col("indicator_value"))
                                )
                        ).as("values")
                );

        Dataset<Row> finalData = groupedIndicators
                .withColumn("symbol", lit(symbol))
                .withColumn("date_bucket", lit(dateBucket))
                .withColumn("parameters", extractParameters(col("indicator_type")));

        // 5. Écrire dans Cassandra
        finalData.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "stock_keyspace")
                .option("table", "technical_indicators")
                .mode("append")
                .save();
    }

    private Column extractIndicatorType(Column colName) {
        return when(colName.like("SMA_%"), lit("SMA"))
                .when(colName.like("bollinger_%"), lit("BOLLINGER"))
                .when(colName.like("RSI_%"), lit("RSI"))
                .when(colName.like("MACD_%"), lit("MACD"))
                .when(colName.like("stochastic_%"), lit("STOCHASTIC"))
                .when(colName.like("OBV_%"), lit("OBV"))
                .otherwise(lit("OTHER"));
    }

    private Column extractValueType(Column colName) {
        return when(colName.like("%_upper"), lit("upper"))
                .when(colName.like("%_lower"), lit("lower"))
                .when(colName.like("%_middle"), lit("middle"))
                .when(colName.like("%_signal"), lit("signal"))
                .when(colName.like("%_histogram"), lit("histogram"))
                .when(colName.like("%_k"), lit("k"))
                .when(colName.like("%_d"), lit("d"))
                .otherwise(lit("value"));
    }

    private Column extractParameters(Column indicatorType) {
        // Créer une map avec les paramètres utilisés pour chaque type d'indicateur
        return when(indicatorType.equalTo("SMA"),
                map(lit("period"), lit(20.0)))
                .when(indicatorType.equalTo("BOLLINGER"),
                        map(lit("period"), lit(20.0), lit("stddev"), lit(2.0)))
                .when(indicatorType.equalTo("RSI"),
                        map(lit("period"), lit(14.0)))
                .when(indicatorType.equalTo("MACD"),
                        map(lit("fast"), lit(12.0), lit("slow"), lit(26.0), lit("signal"), lit(9.0)))
                .otherwise(map());
    }

    private boolean isIndicatorColumn(String colName) {
        return INDICATOR_PREFIXES.stream().anyMatch(colName::startsWith);
    }
}