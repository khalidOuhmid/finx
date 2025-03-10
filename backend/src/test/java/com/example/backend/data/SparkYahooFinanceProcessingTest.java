package com.example.backend.data;

import backend.api.client.YahooFinanceService;
import backend.data.cassandra.ECassandraTables;
import backend.data.sparkdataprocessing.SparkDataProcessor;
import backend.data.sparkdataprocessing.SparkYahooFinanceProcessing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {SparkYahooFinanceProcessing.class, SparkYahooFinanceProcessingTest.TestConfig.class})
@TestPropertySource(properties = { "spring.cassandra.keyspace-name=stock_keyspace" })
@Service
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
                .config("spark.sql.warehouse.dir", "file://" + System.getProperty("java.io.tmpdir") + "/spark-warehouse")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
                .config("spark.driver.host", "localhost")
                .config("spark.executor.memory", "1g")
                .config("spark.driver.memory", "1g")
                .config("spark.driver.extraClassPath", System.getProperty("java.class.path"))
                .config("spark.executor.extraClassPath", System.getProperty("java.class.path"))
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,javax.servlet:javax.servlet-api:4.0.1")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.connection.port", "9042")
                .config("spark.cassandra.auth.username", "")
                .config("spark.cassandra.auth.password", "")
                .config("spark.cassandra.connection.datacenter", "datacenter1")
                .config("spark.metrics.conf.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "false")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDownSpark() {
        if (sparkSession != null) {
            sparkSession.stop();
            System.clearProperty("spark.driver.extraJavaOptions");
        }
    }

    @Test
    void testYahooFinanceProcessing_Success() {
        // Arrange
        String company = "AMZN";
        String range = "1d";
        String mockJson = "{\"chart\":{\"result\":[{\"meta\":{\"symbol\":\"AMZN\"},\"timestamp\":[1740690000],\"indicators\":{\"quote\":[{\"open\":[218.49],\"close\":[219.49],\"high\":[219.5],\"low\":[218.01],\"volume\":[2257200]}]}]}]}";

        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(mockJson);

            // Act
            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            assertEquals(1, result.count());
            Row firstRow = result.first();
            assertEquals("AMZN", firstRow.getAs("company_symbol"));
            assertNotNull(firstRow.getAs("timestamp"));
            assertEquals(218.49, firstRow.getAs("open"), 0.001);
            assertEquals(219.49, firstRow.getAs("close"), 0.001);
        }
    }

    @Test
    void testWriteToCassandra_Success() {
        // Arrange
        Dataset<Row> mockDF = mock(Dataset.class);
        ECassandraTables table = ECassandraTables.raw_stock_data;

        DataFrameWriter<Row> mockWriter = mock(DataFrameWriter.class);
        when(mockDF.write()).thenReturn(mockWriter);
        when(mockWriter.format(anyString())).thenReturn(mockWriter);
        when(mockWriter.option(anyString(), anyString())).thenReturn(mockWriter);
        when(mockWriter.mode(anyString())).thenReturn(mockWriter);
        doNothing().when(mockWriter).save();

        // Act & Assert
        assertDoesNotThrow(() -> processor.writeDataSetInCassandra(table, mockDF));

        // Vérifier que la méthode save() a bien été appelée
        verify(mockWriter, times(1)).save();

        // Vérifier que le format correct est utilisé pour Cassandra
        verify(mockWriter).format("org.apache.spark.sql.cassandra");

    }

    @Configuration
    static class TestConfig {
        @Bean
        @Primary
        public SparkDataProcessor sparkDataProcessor() {
            return mock(SparkDataProcessor.class);
        }
    }
}
