package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkOnBalanceVolumeIndicator implements ISparkIndicator {
    private final String closeColumn;
    private final String volumeColumn;

    public SparkOnBalanceVolumeIndicator(String closeColumn, String volumeColumn) {
        this.closeColumn = closeColumn;
        this.volumeColumn = volumeColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        String obvColumn = "OBV";

        long negativeVolumeCount = timeSeriesData
                .filter(functions.col(volumeColumn).lt(0))
                .count();

        if (negativeVolumeCount > 0) {
            throw new IllegalArgumentException("Dataset contains negative volume values");
        }

        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        WindowSpec prevWindow = Window.orderBy("timestamp");

        // Ajouter la colonne de prix précédent
        Dataset<Row> withPrevClose = sortedData
                .withColumn("prev_close", functions.lag(functions.col(closeColumn), 1).over(prevWindow));

        // Calculer la contribution quotidienne au OBV
        Dataset<Row> withDailyOBV = withPrevClose
                .withColumn("daily_obv",
                        functions.when(functions.col(closeColumn).gt(functions.col("prev_close")),
                                        functions.col(volumeColumn))
                                .when(functions.col(closeColumn).lt(functions.col("prev_close")),
                                        functions.col(volumeColumn).multiply(-1))
                                .otherwise(0));

        // Initialiser l'OBV et accéder à l'OBV précédent
        Dataset<Row> withObvInit = withDailyOBV
                .withColumn(obvColumn, functions.col("daily_obv"))
                .withColumn("prev_obv", functions.lag(functions.col(obvColumn), 1).over(prevWindow));

        // Calculer l'OBV cumulatif
        Dataset<Row> result = withObvInit
                .withColumn(obvColumn,
                        functions.when(functions.col("prev_obv").isNull(), functions.col("daily_obv"))
                                .otherwise(functions.col("prev_obv").plus(functions.col("daily_obv")))
                );

        // Nettoyer les colonnes temporaires
        return result.drop("prev_close", "daily_obv", "prev_obv");
    }

    @Override
    public String getIndicatorType() {
        return "OBV";
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "closeColumn", closeColumn,
                "volumeColumn", volumeColumn
        );
    }
}
