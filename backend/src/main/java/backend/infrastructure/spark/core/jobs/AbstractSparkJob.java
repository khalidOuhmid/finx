package backend.infrastructure.spark.core.jobs;

import backend.infrastructure.cassandra.repository.ECassandraTables;
import backend.infrastructure.spark.config.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Map;

abstract public class AbstractSparkJob implements ISparkJob {
    protected final SparkSession spark;

    public AbstractSparkJob(SparkSessionProvider  sparkProvider) {
        this.spark = sparkProvider.session();
    }
    public abstract void execute(Map<String,Object> params);



    protected boolean validateStructure(Dataset<Row> dataFrame, String[] requiredColumns) {
        for(String requiredColumn : requiredColumns) {
            if(!Arrays.asList(dataFrame.schema().fieldNames()).contains(requiredColumn)) {
                return false;
            }
        }
        System.out.println("[SPARK] -- DataFrame validated");
        return true;
    }

    protected boolean writeDataSet(ECassandraTables tableName, Dataset<Row> dataSet) {
        try {
            if (dataSet.count() == 0) {
                System.out.println("DataFrame vide, aucune écriture effectuée pour " + tableName);
                return false;
            }
            dataSet.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "stock_keyspace")
                    .option("table", tableName.toString())
                    .mode("append")
                    .save();

            System.out.println("Écriture réussie dans " + tableName + " avec " + dataSet.count() + " enregistrements");
            return true;
        }
        catch (Exception e) {
            System.err.println("[CASSANDRA] -- Échec d'écriture pour la table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }


    public SparkSession getSpark() {
        return spark;
    }



}
