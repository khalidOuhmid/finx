package backend.data.cassandra;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.sql.Timestamp;

@Table("companies")
public class Company {
    @Id
    @PrimaryKey
    private  String company_symbol;
    private String company_name;
    private String stock_index;
    private String sector;
    private String country;
    private String ipo_date;

    public String getCompany_symbol() {
        return company_symbol;
    }

    public void setCompany_symbol(String company_symbol) {
        this.company_symbol = company_symbol;
    }

    public String getCompany_name() {
        return company_name;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public String getStock_index() {
        return stock_index;
    }

    public void setStock_index(String stock_index) {
        this.stock_index = stock_index;
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getIpo_date() {
        return ipo_date;
    }

    public void setIpo_date(String ipo_date) {
        this.ipo_date = ipo_date;
    }
}
