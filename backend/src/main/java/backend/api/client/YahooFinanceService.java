package api.client;

import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
@Component
public class YahooFinanceService {

    public static void main(String[] args) throws Exception {
        System.out.println(getGlobalQuote("AMZN", "1d"));
    }

    public static String getGlobalQuote(String company, String range) {
        String url = String.format(
                "https://query1.finance.yahoo.com/v8/finance/chart/%s?interval=1d&range=%s",
                company, range
        );
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

        if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new RuntimeException("Failed to fetch data from Yahoo finance API");
            }
        }
    }

