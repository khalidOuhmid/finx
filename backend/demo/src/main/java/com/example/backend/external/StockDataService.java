package com.example.backend.external;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class StockDataService {

    @Autowired
    private AlphaVantageClient alphaVantageClient;

    @Cacheable(value = "stockPriceCache", key = "#symbol")
    public String getStockPrice(String symbol) {
        System.out.println("GET_GLOBAL_QUOTE");
        return alphaVantageClient.getGlobalQuote(symbol);
    }
    @Cacheable(value = "stockHistoryCache", key = "#symbol")
    public String getHistoricalData(String symbol) {
        System.out.println("GET_HISTORICAL_DATA");
        return alphaVantageClient.getDailyTimeSeries(symbol);
    }

}
