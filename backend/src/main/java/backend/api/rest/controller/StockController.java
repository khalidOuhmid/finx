package api.rest.controller;

import api.client.YahooFinanceService;
import data.sparkdataprocessing.SparkDataProcessor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/stocks")
public class StockController {
    private final YahooFinanceService yahooService;
    private final SparkDataProcessor sparkProcessor;

    public StockController(YahooFinanceService yahooService, SparkDataProcessor sparkProcessor) {
        this.yahooService = yahooService;
        this.sparkProcessor = sparkProcessor;
    }

    @GetMapping("/process/{symbol}")
    public ResponseEntity<String> processStock(@PathVariable String symbol) {
        try{
            String jsonData = yahooService.getGlobalQuote(symbol,"1d");
            return new ResponseEntity<>(jsonData, HttpStatus.OK);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body("Erreure de traitement : " + e.getMessage());
        }
    }
}
