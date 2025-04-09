package com.example.backend.data;

import backend.infrastructure.spark.indicators.SparkBollingerBandsIndicator;
import backend.infrastructure.spark.indicators.SparkMovingAverageIndicator;
import backend.infrastructure.spark.indicators.SparkRsiIndicator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SparkIndicatorsTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .appName("IndicatorsTest")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        spark.sparkContext().setCheckpointDir("/tmp/spark-test-checkpoint");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private Dataset<Row> createTestDataset() {
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
                DataTypes.createStructField("open", DataTypes.DoubleType, false),
                DataTypes.createStructField("high", DataTypes.DoubleType, false),
                DataTypes.createStructField("low", DataTypes.DoubleType, false),
                DataTypes.createStructField("close", DataTypes.DoubleType, false),
                DataTypes.createStructField("volume", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create(Timestamp.valueOf("2025-01-01 10:00:00"), 100.0, 102.0, 99.0, 101.0, 1000L),
                RowFactory.create(Timestamp.valueOf("2025-01-01 10:01:00"), 101.0, 103.0, 100.0, 102.0, 1500L),
                RowFactory.create(Timestamp.valueOf("2025-01-01 10:02:00"), 102.0, 104.0, 101.0, 103.0, 2000L),
                RowFactory.create(Timestamp.valueOf("2025-01-01 10:03:00"), 103.0, 105.0, 102.0, 104.0, 1800L),
                RowFactory.create(Timestamp.valueOf("2025-01-01 10:04:00"), 104.0, 106.0, 103.0, 105.0, 2200L)
        );

        return spark.createDataFrame(rows, schema);
    }

    @Test
    void testBollingerBandsIndicator() {
        Dataset<Row> testData = createTestDataset();

        assertEquals(5, testData.count(), "Le dataset de test devrait avoir 5 lignes");

        SparkBollingerBandsIndicator indicator = new SparkBollingerBandsIndicator(3, "close");

        Dataset<Row> result = indicator.calculate(testData);

        assertEquals(testData.count(), result.count(), "Le résultat devrait avoir le même nombre de lignes que l'entrée");

        List<String> columns = Arrays.asList(result.columns());
        assertTrue(columns.contains("bollinger_middle"), "La colonne bollinger_middle devrait exister");
        assertTrue(columns.contains("bollinger_upper"), "La colonne bollinger_upper devrait exister");
        assertTrue(columns.contains("bollinger_lower"), "La colonne bollinger_lower devrait exister");

        result.cache();

        Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

        Double middleBand = lastRow.getAs("bollinger_middle");
        Double upperBand = lastRow.getAs("bollinger_upper");
        Double lowerBand = lastRow.getAs("bollinger_lower");

        assertNotNull(middleBand, "La bande médiane ne devrait pas être nulle");
        assertNotNull(upperBand, "La bande supérieure ne devrait pas être nulle");
        assertNotNull(lowerBand, "La bande inférieure ne devrait pas être nulle");

        // Pour des données en tendance, la bande médiane devrait être proche de la moyenne des derniers prix
        double expectedMiddle = (103.0 + 104.0 + 105.0) / 3.0;
        assertEquals(expectedMiddle, middleBand, 0.001, "La bande médiane devrait être la moyenne des 3 derniers prix");

        // La bande supérieure devrait être au-dessus de la médiane
        assertTrue(upperBand > middleBand, "La bande supérieure devrait être au-dessus de la médiane");

        // La bande inférieure devrait être en-dessous de la médiane
        assertTrue(lowerBand < middleBand, "La bande inférieure devrait être en-dessous de la médiane");

        // Libérer le cache
        result.unpersist();
    }

    @Test
    void testMovingAverageIndicator() {
        // Créer les données de test
        Dataset<Row> testData = createTestDataset();

        // Créer l'indicateur
        SparkMovingAverageIndicator indicator = new SparkMovingAverageIndicator(3, "close");

        // Calculer l'indicateur
        Dataset<Row> result = indicator.calculate(testData);

        // Vérifier que le résultat a le même nombre de lignes que l'entrée
        assertEquals(testData.count(), result.count(), "Le résultat devrait avoir le même nombre de lignes que l'entrée");

        // Vérifier la colonne générée
        assertTrue(Arrays.asList(result.columns()).contains("SMA_3"), "La colonne SMA_3 devrait exister");

        // Cache le résultat pour accélérer les accès
        result.cache();

        // Récupérer la dernière ligne (plus récente)
        Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

        // Vérifier que la valeur existe et n'est pas nulle
        Double sma = lastRow.getAs("SMA_3");
        assertNotNull(sma, "La valeur SMA ne devrait pas être nulle");

        // Pour les 3 derniers points (103, 104, 105), la moyenne devrait être 104
        double expectedSMA = (103.0 + 104.0 + 105.0) / 3.0;
        assertEquals(expectedSMA, sma, 0.001, "La SMA devrait être la moyenne des 3 derniers prix de clôture");

        // Libérer le cache
        result.unpersist();
    }

    @Test
    void testRsiIndicator() {
        // Créer les données de test
        Dataset<Row> testData = createTestDataset();

        // Créer l'indicateur
        SparkRsiIndicator indicator = new SparkRsiIndicator(3, "close");

        // Calculer l'indicateur
        Dataset<Row> result = indicator.calculate(testData);

        // Vérifier que le résultat a le même nombre de lignes que l'entrée
        assertEquals(testData.count(), result.count(), "Le résultat devrait avoir le même nombre de lignes que l'entrée");

        // Vérifier la colonne générée
        assertTrue(Arrays.asList(result.columns()).contains("RSI_3"), "La colonne RSI_3 devrait exister");

        // Cache le résultat pour accélérer les accès
        result.cache();

        // Pour une tendance haussière constante, le RSI devrait être élevé (>50)
        Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

        // Vérifier que la valeur existe et n'est pas nulle
        Double rsi = lastRow.getAs("RSI_3");
        assertNotNull(rsi, "La valeur RSI ne devrait pas être nulle");

        // Pour une tendance haussière constante, le RSI devrait être proche de 100
        // Note: nous vérifions simplement qu'il est > 50 pour plus de robustesse
        assertTrue(rsi > 50.0,
                "Le RSI devrait être supérieur à 50 pour une tendance haussière (valeur actuelle: " + rsi + ")");

        // Des vérifications supplémentaires pourraient être ajoutées ici
        // Par exemple, pour des données en tendance haussière continue, le RSI devrait être proche de 100

        // Libérer le cache
        result.unpersist();
    }
}
