package data.sparkdataprocessing;

import api.client.YahooFinanceService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

@Service
public class SparkYahooFinanceProcessing {

    public void YahooFinanceProcessing(SparkDataProcessor spark,String company, String range) {
        try{
        String jsonData = YahooFinanceService.getGlobalQuote(company, range);
        Dataset<Row> dataFrame = spark.getSpark().read().json(jsonData);
        Dataset<Row> normalizedDataFrame;
        //TODO
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
