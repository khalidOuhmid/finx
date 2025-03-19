package backend.api.rest.controller;

import backend.api.client.YahooFinanceService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/stocks")
public class StockController {

    @GetMapping("/process/{symbol}")
    public ResponseEntity<String> processStock(@PathVariable String symbol) {
        try{
            String jsonData = YahooFinanceService.getGlobalQuote(symbol,"1d","1min");
            return new ResponseEntity<>(jsonData, HttpStatus.OK);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body("Erreure de traitement : " + e.getMessage());
        }
    }
}
