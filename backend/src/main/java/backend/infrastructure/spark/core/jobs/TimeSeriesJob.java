package backend.infrastructure.spark.core.jobs;
import backend.api.client.YahooFinanceService;
import backend.infrastructure.cassandra.repository.ECassandraTables;
import backend.infrastructure.spark.config.SparkSessionProvider;
import backend.infrastructure.spark.core.pipeline.DataPipeline;
import backend.infrastructure.spark.core.pipeline.timeseries.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.*;

@Slf4j
public class TimeSeriesJob extends AbstractSparkJob {

    public TimeSeriesJob(SparkSessionProvider sparkProvider) {
        super(sparkProvider);
    }

    @Override
    public void execute(Map<String, Object> params) {
        try {
            String[] requiredColumns = {"timestamp", "open", "high", "low", "close", "volume"};
            Dataset<Row> rawData = loadData(params);
            System.out.println("Data loaded");

            String symbol = (String) params.get("company");

            DataPipeline pipeline = new DataPipeline();
            pipeline.addStage(new ValidateStructureStage(requiredColumns))
                    .addStage(new CheckAndFixCorruptRecordStage())
                    .addStage(new StructureExtractionStage(symbol))
                    .addStage(new NullValueCleansingStage(requiredColumns))
                    .addStage(new DropDuplicatesStage())
                    .addStage(new TimestampValidationStage(symbol))
                    .addStage(new EmptyDatasetValidationStage(symbol))
                    .addStage(new ArraySizeValidationStage());

            Dataset<Row> processedData = pipeline.execute(rawData);

            writeDataSet(ECassandraTables.STOCK_DATA, processedData);

        } catch (Exception e) {
            System.err.println("Error while stock data treatment: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Dataset<Row> loadData(Map<String, Object> params) throws Exception {
        String sourceType = (String) params.getOrDefault("sourceType", "json");
        String company = (String) params.get("company");
        String interval = (String) params.get("interval");
        String range = (String) params.get("range");
        String jsonData = YahooFinanceService.getGlobalQuote(company, interval, range);
        Dataset<String> jsonDataset = super.getSpark().createDataset(
                Collections.singletonList(jsonData),
                Encoders.STRING()
        );
        Dataset<Row> rawData = super.getSpark().read()
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(jsonDataset);

        System.out.println("Structure JSON pour " + company + ":");
        rawData.printSchema();

        return rawData;
    }



}
