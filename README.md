# STOCKPIPELINE
A Dockerized data pipeline built with Dagster that fetches stock data from Alpha Vantage API, parses it, and stores it into a PostgreSQL database. The pipeline runs on a schedule (hourly or daily) with error handling, retries, and environment-based configuration.


## What It Does
- Fetches stock prices (hourly or daily)  
- Runs on a schedule with Dagster  
- Saves results into Postgres  
- Easy setup using Docker  

## How to Run

1. Download this project.  

2, Open in Vscode

3. Edit `.env` and add your Alpha Vantage API key:
   
   ALPHAVANTAGE_API_KEY= Enter key here
   STOCK_SYMBOL=IBM/AAPL
   FETCH_INTERVAL=daily/hourly
   

4. Start everything with:
   docker compose up -d --build
   

5. Open Dagster UI at [http://localhost:3000](http://localhost:3000).  
   - Run the job manually Launchpad -> Launch Run.  
    

6. Check data in Postgres:
   bash
   docker compose exec -it postgres psql -U stocks -d stocks -c "SELECT * FROM market.daily_prices LIMIT 10;"
   

## Notes
- Alpha Vantage key  
- If ports `3000` (Dagster) or `5432` (Postgres) are already used, change them in `docker-compose.yml`  
- Database writes are safe to re-run (no duplicates)  

