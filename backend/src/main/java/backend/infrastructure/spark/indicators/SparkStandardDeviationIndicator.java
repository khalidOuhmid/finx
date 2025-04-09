package backend.infrastructure.spark.indicators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Map;

public class SparkStandardDeviationIndicator implements ISparkIndicator {
    private final int period;
    private final String priceColumn;

    public SparkStandardDeviationIndicator(int period, String priceColumn) {
        if (period <= 0) {
            throw new IllegalArgumentException("Period must be greater than zero");
        }
        this.period = period;
        this.priceColumn = priceColumn;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> timeSeriesData) {
        // Nom de la colonne de sortie
        String stdDevColumn = "StdDev_" + period;

        // Trier les données par timestamp
        Dataset<Row> sortedData = timeSeriesData.orderBy("timestamp");

        // Définir la fenêtre glissante pour le calcul
        WindowSpec windowSpec = Window.orderBy("timestamp")
                .rowsBetween(-(period-1), 0);

        // Étape 1: Calculer la SMA pour référence (optionnel)
        Dataset<Row> withSMA = sortedData.withColumn(
                "SMA_" + period,
                functions.avg(functions.col(priceColumn)).over(windowSpec)
        );

        // Étape 2: Calculer l'écart-type directement avec la fonction native de Spark
        Dataset<Row> result = withSMA.withColumn(
                stdDevColumn,
                functions.stddev(functions.col(priceColumn)).over(windowSpec)
        );

        // Option: Si vous voulez conserver la SMA, ne supprimez pas cette colonne
        // Pour rester fidèle à l'implémentation d'origine qui calculait l'écart-type basé sur la SMA
        return result;
    }

    @Override
    public String getIndicatorType() {
        return "StdDev_" + period;
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of(
                "period", period,
                "priceColumn", priceColumn
        );
    }
}
