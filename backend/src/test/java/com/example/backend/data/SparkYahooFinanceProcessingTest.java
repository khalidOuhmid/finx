package com.example.backend.data;

import backend.api.client.YahooFinanceService;
import backend.data.cassandra.ECassandraTables;
import backend.data.sparkdataprocessing.SparkDataProcessor;
import backend.data.sparkdataprocessing.SparkYahooFinanceProcessing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {SparkYahooFinanceProcessing.class, SparkYahooFinanceProcessingTest.TestConfig.class})
public class SparkYahooFinanceProcessingTest {

    private static SparkSession sparkSession;
    @Autowired
    private SparkYahooFinanceProcessing processor;

    @Autowired
    private SparkDataProcessor sparkDataProcessor;

    @BeforeAll
    public static void setupSpark() {
        sparkSession = SparkSession.builder()
                .appName("TestSparkYahooFinanceProcessing")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDownSpark() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    void testYahooFinanceProcessing_Success() {
        // Arrange
        String company = "AMZN";
        String range = "1d";
        String mockJson = "{\"chart\":{\"result\":[{\"meta\":{\"symbol\":\"AMZN\"},\"timestamp\":[1740690000],\"indicators\":{\"quote\":[{\"open\":[218.49],\"close\":[219.49],\"high\":[219.5],\"low\":[218.01],\"volume\":[2257200]}]}]}]}";

        // Configurer le mock pour retourner notre SparkSession locale.
        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        // Simuler l'appel statique vers YahooFinanceService
        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(mockJson);

            // Act
            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            // Assert
            // Vérifier que le DataFrame contient 1 enregistrement
            assertEquals(1, result.count());
            Row firstRow = result.first();
            assertEquals("AMZN", firstRow.getAs("company_symbol"));
            assertNotNull(firstRow.getAs("timestamp"));
            assertEquals(218.49, firstRow.getAs("open"));
            assertEquals(219.49, firstRow.getAs("close"));
        }
    }

    @Test
    void testWriteToCassandra_Success() {
        // Arrange
        Dataset<Row> mockDF = mock(Dataset.class);
        ECassandraTables table = ECassandraTables.raw_stock_data;

        // Créer un mock pour le DataFrameWriter afin de simuler les appels en chaîne
        DataFrameWriter<Row> mockWriter = mock(DataFrameWriter.class);
        when(mockDF.write()).thenReturn(mockWriter);
        when(mockWriter.format(anyString())).thenReturn(mockWriter);
        when(mockWriter.option(anyString(), anyString())).thenReturn(mockWriter);
        when(mockWriter.mode(anyString())).thenReturn(mockWriter);
        doNothing().when(mockWriter).save();

        // Act & Assert : aucun exception ne doit être levée
        assertDoesNotThrow(() -> processor.writeDataSetInCassandra(table, mockDF));
        verify(mockWriter, times(1)).save();
    }

    @Configuration
    static class TestConfig {
        // Fournir un bean mock pour SparkDataProcessor afin qu'il soit injecté dans SparkYahooFinanceProcessing
        @Bean
        @Primary
        public SparkDataProcessor sparkDataProcessor() {
            return mock(SparkDataProcessor.class);
        }
    }
}
