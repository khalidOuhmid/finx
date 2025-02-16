package com.example.backend.service.datacollector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
@Component
public class YahooFinanceService {

    public void main(String[] args) throws Exception {
        System.out.println(getGlobalQuote("AMZN","1d"));
    }

    public  String getGlobalQuote(String company, String range) {
        String url = String.format(
                "https://query1.finance.yahoo.com/v8/finance/chart/%s?interval=1d&range=%s",
                company,range
        );
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            return response.getBody();
        } else {
            throw new RuntimeException("Failed to fetch data from Yahoo finance API");
        }
    }

}
