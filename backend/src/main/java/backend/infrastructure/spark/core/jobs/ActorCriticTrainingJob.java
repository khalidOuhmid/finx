package backend.infrastructure.spark.core.jobs;


import backend.infrastructure.ai.SelfActorCritic;
import backend.infrastructure.ai.TradingEnvironment;
import backend.infrastructure.spark.config.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Imports Spark
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

// Imports DeepLearning4J pour les réseaux de neurones
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;

// Imports ND4J pour manipuler les tenseurs
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

// Imports Java standard
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import backend.infrastructure.ai.SelfActorCritic;
import backend.infrastructure.ai.TradingEnvironment;
//import backend.infrastructure.ai.data.SparkFeatureExtractor;

public class ActorCriticTrainingJob extends AbstractSparkJob  {
    private final TradingEnvironment environment;
    private SelfActorCritic model;

    public ActorCriticTrainingJob(SparkSessionProvider sparkProvider) {
        super(sparkProvider);
        this.environment = new TradingEnvironment(sparkProvider);
    }
    @Override
    public void execute(Map<String, Object> params) {
        // Extraire les paramètres nécessaires
        String symbol = (String) params.get("symbol");
        String dateBucket = (String) params.get("dateBucket");
        int episodes = (int) params.getOrDefault("episodes", 100);
        int stateSize = (int) params.getOrDefault("stateSize", 30);
        int actionSize = 3; // vendre, conserver, acheter
        double learningRate = (double) params.getOrDefault("learningRate", 0.001);
        double gamma = (double) params.getOrDefault("gamma", 0.99); // facteur d'actualisation

        if (model == null) {
            model = new SelfActorCritic(stateSize, actionSize, learningRate, gamma);
        }

        List<Double> episodeRewards = new ArrayList<>();
        System.out.println("Starting training for  " + symbol +
                " on date bucket  " + dateBucket + " for " + episodes + " episodes");

        try{
            for(int episode = 0; episode < episodes; episodes++){
                //Reset environnement
                Dataset<Row> state = environment.reset(symbol,dateBucket);
                boolean done = false;
                double totalReward = 0;

                //Convert dataset to a state vector
                SparkFeatureExtractor extractor = new SparkFeatureExtractor(getSpark());
                INDArray stateVector = extractor.convertToStateVector(state);

                while (!done) {
                    INDArray actionProbs = model.getActionProbabilities(stateVector);
                }
            }
        }
    }
}
