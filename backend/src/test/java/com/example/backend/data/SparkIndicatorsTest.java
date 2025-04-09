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
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    // Méthode utilitaire pour créer un jeu de données de test
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
        // Créer les données de test
        Dataset<Row> testData = createTestDataset();

        // Créer l'indicateur
        SparkBollingerBandsIndicator indicator = new SparkBollingerBandsIndicator(3, "close");

        // Calculer l'indicateur
        Dataset<Row> result = indicator.calculate(testData);

        // Vérifier les colonnes générées
        assertTrue(Arrays.asList(result.columns()).contains("bollinger_middle"));
        assertTrue(Arrays.asList(result.columns()).contains("bollinger_upper"));
        assertTrue(Arrays.asList(result.columns()).contains("bollinger_lower"));

        // Pour les 3 premières lignes, les valeurs peuvent être NaN/null car nous avons une période de 3
        // Mais après cela, nous devrions avoir des valeurs
        if (result.count() >= 5) {
            Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

            assertNotNull(lastRow.getAs("bollinger_middle"));
            assertNotNull(lastRow.getAs("bollinger_upper"));
            assertNotNull(lastRow.getAs("bollinger_lower"));

            // Pour des données en tendance, la bande médiane devrait être proche de la moyenne des derniers prix
            double expectedMiddle = (103.0 + 104.0 + 105.0) / 3.0;
            assertEquals(expectedMiddle, lastRow.getAs("bollinger_middle"), 0.001);

            // La bande supérieure devrait être au-dessus de la médiane
            assertTrue((Double)lastRow.getAs("bollinger_upper") > (Double)lastRow.getAs("bollinger_middle"));

            // La bande inférieure devrait être en-dessous de la médiane
            assertTrue((Double)lastRow.getAs("bollinger_lower") < (Double)lastRow.getAs("bollinger_middle"));
        }
    }

    @Test
    void testMovingAverageIndicator() {
        // Créer les données de test
        Dataset<Row> testData = createTestDataset();

        // Créer l'indicateur
        SparkMovingAverageIndicator indicator = new SparkMovingAverageIndicator(3, "close");

        // Calculer l'indicateur
        Dataset<Row> result = indicator.calculate(testData);

        // Vérifier la colonne générée
        assertTrue(Arrays.asList(result.columns()).contains("SMA_3"));

        // Pour les 3 dernières lignes, nous devrions avoir des valeurs SMA
        if (result.count() >= 5) {
            Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

            // Vérifier que la valeur existe
            assertNotNull(lastRow.getAs("SMA_3"));

            // Pour les 3 derniers points (103, 104, 105), la moyenne devrait être 104
            double expectedSMA = (103.0 + 104.0 + 105.0) / 3.0;
            assertEquals(expectedSMA, lastRow.getAs("SMA_3"), 0.001);
        }
    }

    @Test
    void testRsiIndicator() {
        // Créer les données de test
        Dataset<Row> testData = createTestDataset();

        // Créer l'indicateur
        SparkRsiIndicator indicator = new SparkRsiIndicator(3, "close");

        // Calculer l'indicateur
        Dataset<Row> result = indicator.calculate(testData);

        // Vérifier la colonne générée
        assertTrue(Arrays.asList(result.columns()).contains("RSI_3"));

        // Pour une tendance haussière constante, le RSI devrait être élevé (>50)
        if (result.count() >= 5) {
            Row lastRow = result.orderBy(result.col("timestamp").desc()).first();

            // Vérifier que la valeur existe
            assertNotNull(lastRow.getAs("RSI_3"));

            // Pour une tendance haussière constante, le RSI devrait être proche de 100
            assertTrue((Double)lastRow.getAs("RSI_3") > 50.0);
        }
    }
}
