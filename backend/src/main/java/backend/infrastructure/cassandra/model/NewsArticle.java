package backend.infrastructure.cassandra.model;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Data
public class NewsArticle {
    private UUID articleId;
    private LocalDateTime timestamp;
    private String source;
    private String url;
    private String title;
    private String content;
    private List<String> symbols;
    private boolean processed;
}
