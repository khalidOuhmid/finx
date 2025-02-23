package backend.api.rest.repository;

import backend.data.cassandra.StockIndex;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StockIndexRepository extends CassandraRepository<StockIndex,String> {

}
