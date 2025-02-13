
# Finx - Stock Prediction API

## Purpose
This project retrieves and processes financial data—such as real-time stock prices and historical market data—using the Alpha Vantage API. It also leverages Apache Spark for batch data processing and lays the groundwork for integrating predictive models in the future.

## Tech Stack
- **Backend:** Java, Spring Boot
- **Data Processing:** Apache Spark
- **External API:** Alpha Vantage API
- **Build Tool:** Maven

## Architecture
- **Controller Layer:**  
  Exposes REST endpoints (e.g., `/api/stocks/price`, `/api/spark/process`) for interfacing with the application.

- **Service Layer:**  
  Orchestrates business logic by integrating external API calls (via `AlphaVantageClient`) and Spark-based data processing.

- **External Integration:**  
  Contains clients responsible for calling external services (like the Alpha Vantage API).

- **Spark Module:**  
  Manages data ingestion and processing tasks using Apache Spark.

