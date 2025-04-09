package com.example.backend.data;

import backend.infrastructure.spark.config.SparkConfiguration;
import backend.infrastructure.spark.config.SparkSessionProvider;
import backend.infrastructure.spark.core.jobs.TimeSeriesJob;
import backend.infrastructure.spark.core.pipeline.DataPipeline;
import backend.infrastructure.spark.core.pipeline.timeseries.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TimeSeriesProcessingTest {

    private static SparkSession spark;
    private static SparkSessionProvider sparkProvider;
    private String mockJsonData;

    @BeforeAll
    public static void setupSpark() {
        // Créer une configuration pour les tests avec l'UI désactivée
        SparkConfiguration sparkConfig = new SparkConfiguration();

        // Initialiser manuellement les propriétés pour le test
        sparkConfig.setMaster("local[*]");
        sparkConfig.setAppName("FinIATest");

        // Remplacer la sérialisation Kryo par JavaSerializer (qui gère mieux les lambdas)
        // au lieu de: sparkConfig.setSerializerClass("org.apache.spark.serializer.KryoSerializer");

        sparkConfig.setShufflePartitions(2);
        sparkConfig.setExecutorMemory("1g");
        sparkConfig.setCassandraHost("localhost");
        sparkConfig.setCassandraPort("9042");
        sparkConfig.setCassandraKeyspace("stock_keyspace");

        SparkConf conf = sparkConfig.buildSparkConf();
        conf.set("spark.ui.enabled", "false");

        // Utiliser le sérialiseur Java au lieu de Kryo pour éviter les problèmes avec SerializedLambda
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");

        spark = SparkSession.builder().config(conf).getOrCreate();

        SparkSessionProvider mockProvider = mock(SparkSessionProvider.class);
        when(mockProvider.session()).thenReturn(spark);
        sparkProvider = mockProvider;
    }




    @BeforeEach
    public void setup() {
        // JSON de test pour Yahoo Finance
        mockJsonData = "{\"chart\":{\"result\":[{\"meta\":{\"currency\":\"USD\",\"symbol\":\"AMZN\",\"exchangeName\":\"NMS\",\"fullExchangeName\":\"NasdaqGS\",\"regularMarketPrice\":198.89,\"previousClose\":196.59,\"dataGranularity\":\"1h\",\"range\":\"1d\"},\"timestamp\":[1740690000],\"indicators\":{\"quote\":[{\"open\":[218.49],\"close\":[219.49],\"high\":[219.5],\"low\":[218.01],\"volume\":[2257200]}]}}],\"error\":null}}";
    }

    @AfterAll
    public static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void testValidateStructureStage() {
        // Créer un Dataset avec des données valides
        Dataset<String> jsonDataset = spark.createDataset(
                Collections.singletonList(mockJsonData),
                Encoders.STRING()
        );

        Dataset<Row> rawData = spark.read()
                .option("multiline", "true")
                .json(jsonDataset);

        // Créer l'étape de validation
        ValidateStructureStage stage = new ValidateStructureStage(
                new String[]{"chart", "chart.result", "chart.result[0].meta", "chart.result[0].timestamp"}
        );

        // Exécuter l'étape - ne devrait pas lancer d'exception
        Dataset<Row> result = stage.process(rawData);

        // Vérifier que les données sont inchangées
        assertEquals(rawData.count(), result.count());
    }

    @Test
    void testCheckAndFixCorruptRecordStage() {
        // Créer un Dataset avec données corrompues
        String corruptJson = "{\"chart\":{INVALID_JSON_HERE}";

        Dataset<String> jsonDataset = spark.createDataset(
                Collections.singletonList(corruptJson),
                Encoders.STRING()
        );

        Dataset<Row> rawData = spark.read()
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(jsonDataset);

        // Créer l'étape de nettoyage
        CheckAndFixCorruptRecordStage stage = new CheckAndFixCorruptRecordStage();

        // Vérifier que l'étape identifie les enregistrements corrompus
        assertTrue(Arrays.asList(rawData.columns()).contains("_corrupt_record"));

        // Vérifier que l'étape filtre correctement (devrait être vide après)
        Dataset<Row> result = stage.process(rawData);
        assertEquals(0, result.count());
    }

    @Test
    void testStructureExtractionStage() {
        // Créer un Dataset avec des données valides
        Dataset<String> jsonDataset = spark.createDataset(
                Collections.singletonList(mockJsonData),
                Encoders.STRING()
        );

        Dataset<Row> rawData = spark.read()
                .option("multiline", "true")
                .json(jsonDataset);

        // Créer l'étape d'extraction
        StructureExtractionStage stage = new StructureExtractionStage("AMZN");

        // Exécuter l'étape
        Dataset<Row> result = stage.process(rawData);

        // Vérifier que les colonnes attendues existent
        String[] expectedColumns = {"company_symbol", "timestamps", "opens", "closes", "highs", "lows", "volumes"};
        for (String column : expectedColumns) {
            assertTrue(Arrays.asList(result.columns()).contains(column), "Colonne manquante: " + column);
        }

        // Vérifier les valeurs extraites si le résultat n'est pas vide
        if (result.count() > 0) {
            Row firstRow = result.first();
            assertEquals("AMZN", firstRow.getAs("company_symbol"));
        }
    }

    @Test
    void testCompletePipeline() {
        // Créer un Dataset avec des données valides
        Dataset<String> jsonDataset = spark.createDataset(
                Collections.singletonList(mockJsonData),
                Encoders.STRING()
        );

        Dataset<Row> rawData = spark.read()
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(jsonDataset);

        // Créer le pipeline complet
        String symbol = "AMZN";
        String[] requiredColumns = {"timestamps", "opens", "highs", "lows", "closes", "volumes"};

        DataPipeline pipeline = new DataPipeline();
        pipeline.addStage(new ValidateStructureStage(new String[]{"chart", "chart.result"}))
                .addStage(new CheckAndFixCorruptRecordStage())
                .addStage(new StructureExtractionStage(symbol));

        // Exécuter le pipeline
        Dataset<Row> result = pipeline.execute(rawData);

        // Vérifications
        assertTrue(result.count() > 0, "Le résultat du pipeline ne devrait pas être vide");
        if (result.count() > 0) {
            Row row = result.first();
            assertEquals(symbol, row.getAs("company_symbol"));
        }
    }

    @Test
    void testTimeSeriesJob() {
        // Créer un mock du SparkSessionProvider
        SparkSessionProvider mockProvider = mock(SparkSessionProvider.class);
        when(mockProvider.session()).thenReturn(spark);

        // Créer une instance de TimeSeriesJob avec le mock
        TimeSeriesJob job = new TimeSeriesJob(mockProvider);

        // Configurer les paramètres
        Map<String, Object> params = new HashMap<>();
        params.put("company", "AMZN");
        params.put("interval", "1d");
        params.put("range", "5d");
        params.put("sourceType", "json");

        // Test seulement que l'exécution ne lève pas d'exception
        // Note: Ceci ne teste pas réellement l'intégration avec YahooFinance
        assertDoesNotThrow(() -> {
            job.execute(params);
        });
    }
}
