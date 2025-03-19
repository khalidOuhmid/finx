package backend.data.dataprocessing.json;

import backend.data.cassandra.ECassandraTables;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import jakarta.annotation.PreDestroy;
import org.apache.spark.SparkSQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import java.util.Arrays;
import java.util.Collections;

public class YahooFinanceProcessor extends AbstractYahooFinanceProcessor {
    public YahooFinanceProcessor(SparkSession spark) {
        super(spark);
    }

    @Override
    public Dataset<Row> yahooFinanceProcessing(String jsonData, String company) {
        try {
            Dataset<String> jsonDataset = super.getSpark().createDataset(
                    Collections.singletonList(jsonData),
                    Encoders.STRING()
            );

            // Lire le JSON avec des options configurées pour la robustesse
            Dataset<Row> dataFrame = super.getSpark().read()
                    .option("multiline", "true")
                    .option("mode", "PERMISSIVE") // Mode permissif pour continuer malgré erreurs
                    .option("columnNameOfCorruptRecord", "_corrupt_record") // Nommer explicitement la colonne
                    // .schema(getYahooFinanceSchema()) // Décommenter pour utiliser un schéma explicite
                    .json(jsonDataset);

            // Déboguer le schéma
            System.out.println("Structure JSON pour " + company + ":");
            dataFrame.printSchema();

            // Vérifier les enregistrements corrompus
            if (Arrays.asList(dataFrame.columns()).contains("_corrupt_record")) {
                System.err.println("Enregistrements corrompus trouvés pour " + company + ":");
                dataFrame.select("_corrupt_record").show(false);

                // Si tout est corrompu, retourner un DataFrame vide
                if (dataFrame.columns().length == 1) {
                    System.err.println("JSON entièrement corrompu pour " + company);
                    return super.createEmptyDataFrame(company);
                }
            }

            // Vérifier que la structure du JSON est valide
            if (!validateYahooFinanceStructure(dataFrame)) {
                System.err.println("Structure JSON invalide pour " + company);
                return createEmptyDataFrame(company);
            }

            // Extraction sécurisée en utilisant coalesce pour gérer les nulls
            Dataset<Row> processedDF = dataFrame.selectExpr(
                    "coalesce(chart.result[0].meta.symbol, '" + company + "') as company_symbol",
                    "chart.result[0].timestamp as timestamps",
                    "chart.result[0].indicators.quote[0].open as opens",
                    "chart.result[0].indicators.quote[0].close as closes",
                    "chart.result[0].indicators.quote[0].high as highs",
                    "chart.result[0].indicators.quote[0].low as lows",
                    "chart.result[0].indicators.quote[0].volume as volumes"
            );

            // Vérifier que les données ne sont pas vides
            if (processedDF.select("timestamps").filter(functions.col("timestamps").isNotNull()).count() == 0) {
                System.err.println("Aucune donnée temporelle trouvée pour " + company);
                return createEmptyDataFrame(company);
            }

            // Vérifier que tous les tableaux ont des dimensions compatibles
            processedDF = processedDF.filter(
                    functions.size(functions.col("timestamps")).gt(0)
                            .and(functions.size(functions.col("opens")).gt(0))
                            .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("opens"))))
                            .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("closes"))))
                            .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("highs"))))
                            .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("lows"))))
                            .and(functions.size(functions.col("timestamps")).equalTo(functions.size(functions.col("volumes"))))
            );

            // Vérifier s'il reste des données après le filtrage
            if (processedDF.count() == 0) {
                System.err.println("Aucune donnée valide après filtrage pour " + company);
                return createEmptyDataFrame(company);
            }

            // Créer le Dataset final en explosant les tableaux
            Dataset<Row> finalDF = processedDF.select(
                            functions.col("company_symbol"),
                            functions.explode(
                                    functions.arrays_zip(
                                            functions.col("timestamps"),
                                            functions.col("opens"),
                                            functions.col("closes"),
                                            functions.col("highs"),
                                            functions.col("lows"),
                                            functions.col("volumes")
                                    )
                            ).alias("data")
                    )
                    .select(
                            functions.col("company_symbol"),
                            functions.from_unixtime(functions.col("data.timestamps"), "yyyy-MM").alias("time_bucket"),
                            functions.expr("CAST(data.timestamps * 1000 AS TIMESTAMP)").alias("timestamp"),
                            functions.col("data.opens").cast(DataTypes.DoubleType).alias("open"),
                            functions.col("data.closes").cast(DataTypes.DoubleType).alias("close"),
                            functions.col("data.highs").cast(DataTypes.DoubleType).alias("high"),
                            functions.col("data.lows").cast(DataTypes.DoubleType).alias("low"),
                            functions.col("data.volumes").cast(DataTypes.LongType).alias("volume")
                    )
                    .withColumn("source", functions.lit("Yahoo Finance"));

            // Filtrer les lignes avec des valeurs null essentielles
            finalDF = finalDF.filter(
                    functions.col("timestamp").isNotNull()
                            .and(functions.col("open").isNotNull())
                            .and(functions.col("close").isNotNull())
            );

            System.out.println("Transformation réussie pour " + company + " avec " + finalDF.count() + " enregistrements");
            return finalDF;

        } catch (Exception e) {
            String errorMessage = "[SPARK] -- Erreur de traitement pour " + company +  ": " + e.getMessage();
            System.err.println(errorMessage);
            e.printStackTrace();

            // Retourner un DataFrame vide mais compatible en cas d'échec
            return createEmptyDataFrame(company);
        }
    }

    @Override
    public boolean writeDataSetInCassandra(ECassandraTables tableName, Dataset<Row> dataFrame) {
        try {
            // Ne pas écrire si le DataFrame est vide
            if (dataFrame.count() == 0) {
                System.out.println("DataFrame vide, aucune écriture effectuée pour " + tableName);
                return false;
            }

            // Effectuer l'écriture dans Cassandra
            dataFrame.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "stock_keyspace")
                    .option("table", tableName.toString())
                    .mode("append")
                    .save();

            System.out.println("Écriture réussie dans " + tableName + " avec " + dataFrame.count() + " enregistrements");
            return true;
        } catch (Exception e) {
            System.err.println("[CASSANDRA] -- Échec d'écriture pour la table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            try {
                String backupPath = "s3://backup-path/" + tableName + "_" + System.currentTimeMillis() + ".parquet";
                dataFrame.write().parquet(backupPath);
                System.out.println("Sauvegarde de secours créée: " + backupPath);
            } catch (Exception backupEx) {
                System.err.println("Échec de la sauvegarde: " + backupEx.getMessage());
            }
            throw new RuntimeException("[CASSANDRA] -- Échec d'écriture pour la table " + tableName, e);
        }
    }

    @Override
    public boolean validateYahooFinanceStructure(Dataset<Row> dataFrame) {
        try {
            // Vérifier les champs obligatoires
            if (!Arrays.asList(dataFrame.schema().fieldNames()).contains("chart")) {
                System.err.println("Erreur de structure: champ 'chart' manquant");
                return false;
            }

            // Vérifier si result existe et n'est pas vide
            // Solution au problème de cast Integer vers Long
            Object countObj = dataFrame.selectExpr("size(chart.result)").first().get(0);
            long resultCount;
            if (countObj instanceof Integer) {
                resultCount = ((Integer) countObj).longValue(); // Conversion sécurisée Integer → Long
            } else {
                resultCount = (Long) countObj;
            }

            if (resultCount == 0) {
                System.err.println("Erreur de structure: 'chart.result' est vide ou null");
                return false;
            }

            // Vérification des timestamps et indicateurs
            try {
                Row checkRow = dataFrame.selectExpr(
                        "size(chart.result[0].timestamp) > 0 as hasTimestamps",
                        "size(chart.result[0].indicators.quote[0].open) > 0 as hasIndicators"
                ).first();

                return checkRow.getBoolean(0) && checkRow.getBoolean(1);
            } catch (Exception e) {
                System.err.println("Erreur de validation des données: " + e.getMessage());
                return false;
            }
        } catch (Exception e) {
            System.err.println("Erreur lors de la validation de la structure: " + e.getMessage());
            return false;
        }
    }
}
