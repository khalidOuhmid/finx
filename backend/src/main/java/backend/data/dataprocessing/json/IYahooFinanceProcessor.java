package backend.data.dataprocessing.json;

import backend.data.cassandra.ECassandraTables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Interface for processing Yahoo Finance financial data.
 * Provides methods to transform JSON data into Spark DataFrames
 * and save them to Cassandra.
 */
public interface IYahooFinanceProcessor {

    /**
     * Processes Yahoo Finance JSON data and converts it to a DataFrame.
     *
     * @param jsonData Raw JSON data from Yahoo Finance API
     * @param company Company symbol
     * @return A DataFrame containing structured financial data
     * @throws IllegalArgumentException if the JSON data is invalid
     */
    Dataset<Row> yahooFinanceProcessing(String jsonData, String company);

    /**
     * Writes the processed DataFrame to a specified Cassandra table.
     *
     * @param tableName The target Cassandra table name
     * @param dataFrame The DataFrame containing the data to write
     * @return true if the write operation was successful, false otherwise
     * @throws IllegalArgumentException if the DataFrame schema doesn't match the expected one
     */
    boolean writeDataSetInCassandra(ECassandraTables tableName, Dataset<Row> dataFrame);

    /**
     * Creates and returns the schema for Yahoo Finance data.
     *
     * @return StructType representing the Yahoo Finance data schema
     */
    StructType getYahooFinanceSchema();

    /**
     * Validates whether the DataFrame has a valid Yahoo Finance structure.
     *
     * @param dataFrame The DataFrame to validate
     * @return true if the data has a valid structure, false otherwise
     */
    boolean validateYahooFinanceStructure(Dataset<Row> dataFrame);

    /**
     * Creates an empty DataFrame with the correct schema for Yahoo Finance data.
     *
     * @param company Company symbol
     * @return An empty DataFrame with the Yahoo Finance schema
     */
    Dataset<Row> createEmptyDataFrame(String company);
}
