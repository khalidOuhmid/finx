package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkStochasticOscillatorIndicator implements ISparkIndicator {
    private final int kPeriod;
    private final int dPeriod;
    private final String closeColumn;
    private final String highColumn;
    private final String lowColumn;

    public SparkStochasticOscillatorIndicator(int kPeriod, int dPeriod, String closeColumn, String highColumn, String lowColumn) {
        this.kPeriod = kPeriod;
        this.dPeriod = dPeriod;
        this.closeColumn = closeColumn;
        this.highColumn = highColumn;
        this.lowColumn = lowColumn;
    }

    public SparkStochasticOscillatorIndicator(String closeColumn, String highColumn, String lowColumn) {
        this(14, 3, closeColumn, highColumn, lowColumn);
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        // Noms des colonnes de sortie
        String kColumn = "Stochastic_%K_" + kPeriod;
        String dColumn = "Stochastic_%D_" + kPeriod + "_" + dPeriod;

        // Trier les données par horodatage
        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        // Définir la fenêtre pour trouver les plus hauts et plus bas sur la période kPeriod
        WindowSpec kWindow = Window.orderBy("timestamp")
                .rowsBetween(-(kPeriod-1), 0);

        // Calculer le plus haut et le plus bas sur la période kPeriod
        Dataset<Row> withHighLow = sortedData
                .withColumn("highest_high", functions.max(functions.col(highColumn)).over(kWindow))
                .withColumn("lowest_low", functions.min(functions.col(lowColumn)).over(kWindow));

        // Calculer %K selon la formule: (ClosePrice - LowestLow) / (HighestHigh - LowestLow) * 100
        Dataset<Row> withK = withHighLow
                .withColumn(kColumn,
                        functions.when(
                                functions.col("highest_high").equalTo(functions.col("lowest_low")),
                                functions.lit(50.0)
                        ).otherwise(
                                functions.col(closeColumn)
                                        .minus(functions.col("lowest_low"))
                                        .divide(
                                                functions.col("highest_high")
                                                        .minus(functions.col("lowest_low"))
                                        )
                                        .multiply(100)
                        )
                );

        WindowSpec dWindow = Window.orderBy("timestamp")
                .rowsBetween(-(dPeriod-1), 0);

        Dataset<Row> result = withK
                .withColumn(dColumn, functions.avg(functions.col(kColumn)).over(dWindow));

        return result.drop("highest_high", "lowest_low");
    }

    @Override
    public String getIndicatorType() {
        return "StochasticOscillator";
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "kPeriod", kPeriod,
                "dPeriod", dPeriod,
                "closeColumn", closeColumn,
                "highColumn", highColumn,
                "lowColumn", lowColumn
        );
    }
}
