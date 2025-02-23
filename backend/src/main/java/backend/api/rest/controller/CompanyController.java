package backend.api.rest.controller;

import backend.api.rest.repository.CompanyRepository;
import backend.data.cassandra.Company;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import java.util.List;
import java.util.NoSuchElementException;

@Controller
@RequestMapping("/api/companies")
public class CompanyController{


    private CompanyRepository companyRepository;


    /**
     *
     * @return
     */
    @GetMapping
    public List<Company> getAllCompanies(){
        return companyRepository.findAll();
    }

    @RequestMapping("/{companySymbol}")
    public Company getCompanyById(@PathVariable String companySymbol){
         return companyRepository.findById(companySymbol)
                 .orElseThrow(() -> new NoSuchElementException("Company not found"));
    }

}
