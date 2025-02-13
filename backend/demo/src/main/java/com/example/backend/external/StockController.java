package com.example.backend.external;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stocks")
public class StockController {

    @Autowired
    private StockDataService stockDataService;

    @GetMapping("/price")
    public ResponseEntity<String> getPrice(@RequestParam String symbol) {
        return ResponseEntity.ok(stockDataService.getStockPrice(symbol));
    }

    @GetMapping("/history")
    public ResponseEntity<String> getHistory(@RequestParam String symbol) {
        return ResponseEntity.ok(stockDataService.getHistoricalData(symbol));
    }
}
