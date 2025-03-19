package com.example.backend.data;

import backend.data.cassandra.ECassandraTables;
import backend.data.dataprocessing.json.IYahooFinanceProcessor;
import backend.data.dataprocessing.json.YahooFinanceProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {YahooFinanceProcessorTest.TestConfig.class})
@TestPropertySource(properties = { "spring.cassandra.keyspace-name=stock_keyspace" })
public class YahooFinanceProcessorTest {

    private static SparkSession sparkSession;
    private static final String DEFAULT_INTERVAL = "1d";

    private IYahooFinanceProcessor processor;
    private String mockJsonData;

    @BeforeAll
    public static void setupSpark() {
        sparkSession = SparkSession.builder()
                .appName("TestYahooFinanceProcessor")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file://" + System.getProperty("java.io.tmpdir") + "/spark-warehouse")
                .config("spark.ui.enabled", "false")
                // Autres configurations comme avant
                .getOrCreate();
    }

    @BeforeEach
    public void setup() {
        processor = new YahooFinanceProcessor(sparkSession);

        // JSON de test pour Yahoo Finance
        mockJsonData = "{\"chart\":{\"result\":[{\"meta\":{\"currency\":\"USD\",\"symbol\":\"AMZN\",\"exchangeName\":\"NMS\",\"fullExchangeName\":\"NasdaqGS\",\"regularMarketPrice\":198.89,\"previousClose\":196.59,\"dataGranularity\":\"1h\",\"range\":\"1d\"},\"timestamp\":[1740690000],\"indicators\":{\"quote\":[{\"open\":[218.49],\"close\":[219.49],\"high\":[219.5],\"low\":[218.01],\"volume\":[2257200]}]}}],\"error\":null}}";
    }

    @AfterAll
    public static void tearDownSpark() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    void testYahooFinanceProcessing_Success() {
        // Utilise directement le JSON mock (ne dépend pas de l'appel à l'API)
        String company = "AMZN";

        Dataset<Row> result = processor.yahooFinanceProcessing(mockJsonData, company);

        // Vérifications
        assertEquals(1, result.count());
        Row firstRow = result.first();
        assertEquals("AMZN", firstRow.getAs("company_symbol"));
        assertNotNull(firstRow.getAs("timestamp"));
        assertEquals(218.49, firstRow.getAs("open"), 0.001);
        assertEquals(219.49, firstRow.getAs("close"), 0.001);
        assertEquals(219.5, firstRow.getAs("high"), 0.001);
        assertEquals(218.01, firstRow.getAs("low"), 0.001);
        assertEquals(2257200.0, ((Long)firstRow.getAs("volume")).doubleValue(), 0.001);
        assertEquals("Yahoo Finance", firstRow.getAs("source"));
    }

    @Test
    void testYahooFinanceProcessing_EmptyJsonHandling() {
        String company = "INVALID";
        String emptyJson = "";

        Dataset<Row> result = processor.yahooFinanceProcessing(emptyJson, company);

        // Vérifier que DataFrame vide est retourné, pas null
        assertNotNull(result);
        assertEquals(0, result.count());
        // Vérifier que le schéma est conforme
        assertTrue(result.schema().fieldNames().length > 0);
        assertTrue(Arrays.asList(result.schema().fieldNames()).contains("company_symbol"));
    }

    @Test
    void testYahooFinanceProcessing_CorruptJsonHandling() {
        String company = "CORRUPT";
        String corruptJson = "{\"chart\":{INVALID_JSON_HERE}";

        Dataset<Row> result = processor.yahooFinanceProcessing(corruptJson, company);

        // Vérifier que DataFrame vide est retourné pour JSON corrompu
        assertNotNull(result);
        assertEquals(0, result.count());
    }

    @Test
    void testYahooFinanceProcessing_InvalidStructureHandling() {
        String company = "WRONG";
        // JSON valide mais structure incorrecte (sans chart.result)
        String invalidStructureJson = "{\"other_data\":{\"value\":123}}";

        Dataset<Row> result = processor.yahooFinanceProcessing(invalidStructureJson, company);

        // Vérifier que DataFrame vide est retourné pour structure invalide
        assertNotNull(result);
        assertEquals(0, result.count());
    }

    @Test
    void testValidateYahooFinanceStructure() {
        // Créer un mock de Dataset avec structure valide
        Dataset<Row> validDataFrame = sparkSession.read()
                .json(sparkSession.createDataset(
                        Arrays.asList(mockJsonData),
                        org.apache.spark.sql.Encoders.STRING()
                ));

        // Tester validation sur données valides
        assertTrue(processor.validateYahooFinanceStructure(validDataFrame));

        // Tester validation sur données invalides
        Dataset<Row> invalidDataFrame = sparkSession.read()
                .json(sparkSession.createDataset(
                        Arrays.asList("{\"wrong_structure\":true}"),
                        org.apache.spark.sql.Encoders.STRING()
                ));

        assertFalse(processor.validateYahooFinanceStructure(invalidDataFrame));
    }

    @Test
    void testWriteToCassandra_Success() {
        // Arrange - créer un mock de Dataset
        Dataset<Row> mockDF = mock(Dataset.class);
        ECassandraTables table = ECassandraTables.raw_stock_data;

        DataFrameWriter<Row> mockWriter = mock(DataFrameWriter.class);
        when(mockDF.write()).thenReturn(mockWriter);
        when(mockWriter.format(anyString())).thenReturn(mockWriter);
        when(mockWriter.option(anyString(), anyString())).thenReturn(mockWriter);
        when(mockWriter.mode(anyString())).thenReturn(mockWriter);
        doNothing().when(mockWriter).save();
        when(mockDF.count()).thenReturn(10L); // Simuler DataFrame non vide

        // Act
        boolean result = processor.writeDataSetInCassandra(table, mockDF);

        // Assert
        assertTrue(result, "L'écriture devrait retourner true pour indiquer le succès");
        verify(mockWriter, times(1)).save();
        verify(mockWriter).format("org.apache.spark.sql.cassandra");
        verify(mockWriter).option("keyspace", "stock_keyspace");
        verify(mockWriter).option("table", table.toString());
    }

    @Test
    void testWriteToCassandra_EmptyDataFrame() {
        // Arrange
        Dataset<Row> mockDF = mock(Dataset.class);
        ECassandraTables table = ECassandraTables.raw_stock_data;
        when(mockDF.count()).thenReturn(0L); // DataFrame vide

        // Act
        boolean result = processor.writeDataSetInCassandra(table, mockDF);

        // Assert
        assertFalse(result, "L'écriture devrait retourner false pour un DataFrame vide");
        // Vérifier qu'aucune opération d'écriture n'est appelée pour un DataFrame vide
        verify(mockDF, never()).write();
    }

    @Test
    void testCreateEmptyDataFrame() {
        String company = "TEST";
        Dataset<Row> emptyDF = processor.createEmptyDataFrame(company);

        assertNotNull(emptyDF);
        assertEquals(0, emptyDF.count());
        // Vérifier le schéma
        assertTrue(Arrays.asList(emptyDF.schema().fieldNames()).contains("company_symbol"));
        assertTrue(Arrays.asList(emptyDF.schema().fieldNames()).contains("timestamp"));
        assertTrue(Arrays.asList(emptyDF.schema().fieldNames()).contains("open"));
        assertTrue(Arrays.asList(emptyDF.schema().fieldNames()).contains("close"));
    }

    @Configuration
    static class TestConfig {
        @Bean
        public SparkSession sparkSession() {
            return sparkSession;
        }
    }
}
