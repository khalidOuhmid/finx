package backend.api.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
@Component
public class AlphaVantageClient {
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${alphavantage.api.key}")
    private String apiKey;

    /**
     *
     * @param symbol
     * @return
     */
    public String getGlobalQuote(String symbol) {
        String url = String.format(
                "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=%s&apikey=%s",
                symbol, apiKey
        );
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            return response.getBody();
        } else {
            throw new RuntimeException("Failed to fetch data from alphavantage API");
        }
    }

    public String getDailyTimeSeries(String symbol) {
        String url = String.format(
        "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=%s&outputsize=compact&apikey=%s",
                symbol,apiKey
                );
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        if(response.getStatusCode() == HttpStatus.OK) {

            return response.getBody();
        }else{
            throw new RuntimeException("Failed to fetch data from alphavantage API");
        }
    }


    }


