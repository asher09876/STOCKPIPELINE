# STOCKPIPELINE
A Dockerized data pipeline built with Dagster that fetches stock data from Alpha Vantage API, parses it, and stores it into a PostgreSQL database. The pipeline runs on a schedule (hourly or daily) with error handling, retries, and environment-based configuration.
