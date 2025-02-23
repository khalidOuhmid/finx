package backend.data.cassandra;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.sql.Timestamp;

@Table("stock_indices")
public class StockIndex {
    @Id
    @PrimaryKey
    private String index_symbol;
    private String country;
    private String description;
    private int company_count;
    private Timestamp last_updated;

    public String getIndex_symbol() {
        return index_symbol;
    }

    public void setIndex_symbol(String index_symbol) {
        this.index_symbol = index_symbol;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getCompany_count() {
        return company_count;
    }

    public void setCompany_count(int company_count) {
        this.company_count = company_count;
    }

    public Timestamp getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Timestamp last_updated) {
        this.last_updated = last_updated;
    }
}
