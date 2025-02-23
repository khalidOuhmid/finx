package backend.api.rest.repository;

import backend.data.cassandra.Company;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CompanyRepository extends CassandraRepository<Company, String> {

}
