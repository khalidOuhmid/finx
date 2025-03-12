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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {SparkYahooFinanceProcessing.class, SparkYahooFinanceProcessingTest.TestConfig.class})
@TestPropertySource(properties = { "spring.cassandra.keyspace-name=stock_keyspace" })
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
        String mockJson = "{\"chart\":{\"result\":[{\"meta\":{\"currency\":\"USD\",\"symbol\":\"AMZN\",\"exchangeName\":\"NMS\",\"fullExchangeName\":\"NasdaqGS\",\"regularMarketPrice\":198.89,\"previousClose\":196.59,\"dataGranularity\":\"1h\",\"range\":\"1d\"},\"timestamp\":[1740690000],\"indicators\":{\"quote\":[{\"open\":[218.49],\"close\":[219.49],\"high\":[219.5],\"low\":[218.01],\"volume\":[2257200]}]}}],\"error\":null}}";


        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(mockJson);

            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            // Verify
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
    }

    @Test
    void testYahooFinanceProcessing_EmptyJsonHandling() {
        // Arrange
        String company = "INVALID";
        String range = "1d";

        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(null);

            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            // Verify empty DataFrame is returned, not null
            assertNotNull(result);
            assertEquals(0, result.count());
            // Verify schema is as expected
            assertTrue(result.schema().fieldNames().length > 0);
            assertTrue(Arrays.asList(result.schema().fieldNames()).contains("company_symbol"));
        }
    }

    @Test
    void testYahooFinanceProcessing_CorruptJsonHandling() {
        // Arrange
        String company = "CORRUPT";
        String range = "1d";
        String corruptJson = "{\"chart\":{INVALID_JSON_HERE}";

        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(corruptJson);

            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            // Verify empty DataFrame is returned for corrupt JSON
            assertNotNull(result);
            assertEquals(0, result.count());
        }
    }

    @Test
    void testYahooFinanceProcessing_InvalidStructureHandling() {
        // Arrange
        String company = "WRONG";
        String range = "1d";
        // Valid JSON but wrong structure (missing chart.result element)
        String invalidStructureJson = "{\"other_data\":{\"value\":123}}";

        when(sparkDataProcessor.getSpark()).thenReturn(sparkSession);

        try (MockedStatic<YahooFinanceService> yahooMock = mockStatic(YahooFinanceService.class)) {
            yahooMock.when(() -> YahooFinanceService.getGlobalQuote(company, range))
                    .thenReturn(invalidStructureJson);

            Dataset<Row> result = processor.yahooFinanceProcessing(company, range);

            // Verify empty DataFrame is returned for invalid structure
            assertNotNull(result);
            assertEquals(0, result.count());
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
        when(mockDF.count()).thenReturn(10L); // Simulate non-empty DataFrame

        // Act & Assert
        assertDoesNotThrow(() -> processor.writeDataSetInCassandra(table, mockDF));

        // Verify
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
        when(mockDF.count()).thenReturn(0L); // Empty DataFrame

        // Act
        processor.writeDataSetInCassandra(table, mockDF);

        // Verify that no write operations are called for empty DataFrame
        verify(mockDF, never()).write();
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
