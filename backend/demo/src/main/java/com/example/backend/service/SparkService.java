package com.example.backend.service;

import com.example.backend.spark.SparkProcessor;
import org.springframework.stereotype.Service;

@Service
public class SparkService {

    private final SparkProcessor sparkProcessor;

    public SparkService() {
        this.sparkProcessor = new SparkProcessor();
    }

    public void processFile(String filePath) {
        sparkProcessor.processData(filePath);
    }

    public void stopSpark() {
        sparkProcessor.stop();
    }
}
