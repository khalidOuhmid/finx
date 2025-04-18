package backend.api.scraper;
import org.apache.zookeeper.Environment;
import org.jsoup.Jsoup;
import backend.infrastructure.cassandra.model.NewsArticle;
import org.jvnet.hk2.annotations.Service;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Logger;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

@Service
public class InvestingScraperService {

    @Value("${chrome.driver.path}")
    private static String chromedriver;
    private static Environment environment;

    public static void main(String[] args) {
        System.out.println(chromedriver);
    }
    // TODO: Implémenter la méthode principale
    private static final Logger logger = Logger.getLogger(InvestingScraperService.class.getName());
    private static final String NEWS_URL = "https://www.investing.com/news/stock-market-news/";

        public List<NewsArticle> scrapeLatestArticles(int maxArticles) {

        return null;
    }

    private NewsArticle extractArticleData(Element articleElement) {
        //TODO
        return null;
}

    private static String fetchArticleContent(String url) {
        try {
            Document articleDoc = getDocument(url);
            Element contentElement = articleDoc.select("div.articlePage").first();
            return contentElement != null ? contentElement.text() : "";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private LocalDateTime parseDateTime(String dateString) {
        //TODO
        return LocalDateTime.parse(dateString);
    }


    private static Document getDocument(String url) throws IOException {
        int maxRetries = 3;
        int retryDelay = 2000; // ms

        for (int i = 0; i < maxRetries; i++) {
            try {
                // Ajout d'un délai entre les tentatives
                if (i > 0) Thread.sleep(retryDelay * i);

                return Jsoup.connect(url)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36") // User-Agent plus récent
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                        .header("Accept-Language", "en-US,en;q=0.5")
                        .header("Accept-Encoding", "gzip, deflate, br")
                        .header("DNT", "1")
                        .header("Connection", "keep-alive")
                        .header("Upgrade-Insecure-Requests", "1")
                        .header("Sec-Fetch-Dest", "document")
                        .header("Sec-Fetch-Mode", "navigate")
                        .header("Sec-Fetch-Site", "none")
                        .header("Cache-Control", "max-age=0")
                        .header("X-Requested-With", "XMLHttpRequest")
                        .referrer("https://www.google.com/")
                        .timeout(10000)
                        .get();
            } catch (IOException e) {
                if (i == maxRetries - 1) throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during retry delay", e);
            }
        }
        throw new IOException("Failed after max retries");
    }

    private static String fetchWithSelenium(String url) {
        // Configurer le driver
        System.setProperty("webdriver.chrome.driver", chromedriver);
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless");
        options.addArguments("--disable-gpu");
        options.addArguments("--window-size=1920,1080");
        options.addArguments("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

        WebDriver driver = new ChromeDriver(options);
        try {
            driver.get(url);
            // Attendre le chargement complet
            Thread.sleep(5000);

            // Obtenir le contenu de la page
            String pageSource = driver.getPageSource();

            // Analyser avec Jsoup
            Document doc = Jsoup.parse(pageSource);
            Element contentElement = doc.select("div.articlePage").first();
            return contentElement != null ? contentElement.text() : "";
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        } finally {
            driver.quit();
        }
    }
}