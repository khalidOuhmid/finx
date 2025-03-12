# AlgoFinIA - Financial Analysis Platform Inspired by Aladdin

## Project Description

AlgoFinIA is a financial analysis platform inspired by BlackRock's Aladdin, developed in Java. The project aims to create a comprehensive system capable of collecting, analyzing, and visualizing financial data, performing Monte Carlo simulations, calculating risk metrics, and optimizing investment portfolios.

Eventually, this platform will offer advanced features such as real-time analysis with NLP, interactive dashboards, and AI-based automated portfolio management.

## Tech Stack

- **Java** - Main development language
- **Spring** - Framework for web applications and infrastructure
- **Apache Spark** - Distributed large-scale data processing
- **Cassandra** - Distributed NoSQL database for storage
- **Deeplearning4j** - Deep learning framework for Java
- **Kafka** - Distributed messaging system for real-time streaming

**Note:** This project is in active development. Features and architecture may change as development progresses.

## Current Progress

| Phase | Goal | Status |
|-------|------|--------|
| Phase 1 | Goal 1: Retrieve and Normalize Yahoo Finance Data | ✅ Completed |
| Phase 1 | Goal 2: Create Cassandra Schema | ✅ Completed |
| Phase 2 | Goal 3: Calculate Technical Indicators | 🔄 In progress |
| Phase 2-6 | Goals 4-12 | ⏳ Planned |

## Detailed Roadmap

### **PHASE 1 : Foundational Technologies**

#### **Goal 1 : Retrieve and Normalize Yahoo Finance Data** ✅
- **Sub-tasks completed:**
  - Configure Yahoo Finance API client
  - Retrieve historical data (price, volume, etc.) over a given period
  - Load data into a Spark DataFrame
  - Normalize columns (timestamp, open, close, high, low, volume)
  - Write normalized data to Cassandra (`market_data_1m`)

#### **Goal 2 : Create Cassandra Schema** ✅
- **Sub-tasks completed:**
  - Design necessary tables:
    - `market_data_1m` for time series
    - `asset_correlations` for correlations between assets
    - `risk_metrics` for Monte Carlo simulations and stress tests
  - Configure keyspace with a simple replication strategy
  - Test writing and reading data in Cassandra

### **PHASE 2 : Quantitative Analysis**

#### **Goal 3 : Calculate Technical Indicators** 🔄
- **Sub-tasks in progress:**
  - Implement each technical indicator:
    - SMA (Simple Moving Average)
    - EMA (Exponential Moving Average)
    - RSI (Relative Strength Index)
    - MACD (EMA12, EMA26, signal line)
    - Bollinger Bands (average + standard deviations)
    - Stochastic Oscillator (%K, %D)
    - OBV (On-Balance Volume)
  - Add these indicators to existing data in `market_data_1m`
  - Validate calculations by comparing with financial tools like TradingView

#### **Goal 4 : Build Correlation Matrices** ⏳
- **Planned sub-tasks:**
  - Calculate correlations between financial assets over different periods (daily, weekly, monthly) with Spark
  - Store results in the Cassandra `asset_correlations` table
  - Visualize correlations as a heatmap (optional)

### **PHASE 3 : Advanced Models and Simulations**

#### **Goal 5 : Implement Monte Carlo Simulations** ⏳
- **Planned sub-tasks:**
  - Generate random scenarios to model future price evolution of assets:
    - Geometric Brownian motion model
  - Parallelize simulations with Spark to improve performance
  - Calculate metrics such as:
    - Value-at-Risk (VaR)
    - Expected Shortfall (ES)
  - Store results in the `risk_metrics` table

#### **Goal 6 : Implement Stress Testing** ⏳
- **Planned sub-tasks:**
  - Build hypothetical scenarios:
    - Interest rate hikes
    - Stock market crash
    - Major geopolitical events
  - Simulate the impact of these scenarios on financial portfolios
  - Store results in the `risk_metrics` table

#### **Goal 7 : Integrate Deeplearning4j for Prediction** ⏳
- **Planned sub-tasks:**
  - Load calculated technical data (SMA, EMA, RSI, etc.) into ND4J
  - Build a simple model based on a dense network or LSTM to predict future prices
  - Evaluate model performance with metrics like RMSE or MAPE
  - Save the trained model for future use

### **PHASE 4 : Real-Time and Automation**

#### **Goal 8 : Configure Kafka for Real-Time Streaming** ⏳
- **Planned sub-tasks:**
  - Configure a Kafka cluster with multiple partitions
  - Create a `market-data-stream` topic to broadcast real-time stock data
  - Implement a Kafka producer to send data retrieved via the Yahoo Finance API
  - Create a Kafka consumer with Spark Structured Streaming to read and process this data

#### **Goal 9 : Automated Portfolio Management** ⏳
- **Planned sub-tasks:**
  - Implement an optimization engine based on the mean/variance model (Markowitz)
  - Add dynamic rules to automatically adjust allocations based on calculated risk or opportunities detected by AI
  - Automate rebalancing via predefined strategies

### **PHASE 5 : Aladdin-Inspired Features**

#### **Goal 10 : Real-Time Analysis with NLP** ⏳
- **Planned sub-tasks:**
  - Integrate an NLP engine (like SpaCy or Apache OpenNLP) to analyze financial news or social networks
  - Implement real-time sentiment analysis to detect positive or negative trends on financial assets
  - Connect data streams to Kafka for real-time processing

#### **Goal 11 : ESG Analysis (Environmental, Social and Governance)** ⏳
- **Planned sub-tasks:**
  - Collect ESG data from external sources (e.g., Refinitiv or Bloomberg ESG API)
  - Normalize this data and calculate an ESG score per company
  - Build an AI-based ESG evaluation model
  - Integrate these scores into investment decisions

### **PHASE 6 : Visualization and Reporting**

#### **Goal 12 : Visual Dashboard** ⏳
- **Planned sub-tasks:**
  - Build an interactive dashboard with Angular or React to visualize:
    - Portfolio performance (return, volatility)
    - Asset correlations as a heatmap
    - Results of stress tests and Monte Carlo simulations
  - Add customizable widgets so users can choose which metrics to display
  - Connect the Spring Boot backend to the frontend via REST/GraphQL APIs



