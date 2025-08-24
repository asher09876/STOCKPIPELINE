import os
import json
from datetime import datetime, timezone

from dagster import (
    Definitions,
    ScheduleDefinition,
    get_dagster_logger,
    RetryPolicy,
    op,
    job,
)
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL_DAILY = "https://www.alphavantage.co/query"


def env(name: str, default: str = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_data(context):
    logger = get_dagster_logger()
    api_key = env("ALPHAVANTAGE_API_KEY")
    symbol = env("STOCK_SYMBOL", "IBM")
    interval = os.getenv("FETCH_INTERVAL", "daily").lower()
    if interval not in {"hourly", "daily"}:
        interval = "daily"
    if interval == "hourly":
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "60min",
            "datatype": "json",
            "apikey": api_key,
        }
    else:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "datatype": "json",
            "apikey": api_key,
        }
    try:
        resp = requests.get(API_URL_DAILY, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        logger.error(f"HTTP error: {e}")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON response.")
        raise
    if "Error Message" in payload or "Note" in payload:
        logger.warning(
            f"API returned a message: {payload.get('Error Message') or payload.get('Note')}"
        )
    return {"symbol": symbol, "payload": payload, "interval": interval}


@op
def parse_timeseries(context, data: dict):
    logger = get_dagster_logger()
    payload = data["payload"]
    symbol = data["symbol"]
    key = next((k for k in payload.keys() if "Time Series" in k), None)
    if key is None or key not in payload:
        logger.warning("No time series found in payload; returning empty list.")
        return []
    series = payload[key]
    rows = []
    for ts_str, metrics in series.items():
        try:
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        except Exception:
            logger.warning(f"Bad timestamp: {ts_str}; skipping")
            continue

        def safe_get(k):
            v = metrics.get(k)
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        try:
            volume_val = (
                int(float(metrics.get("5. volume"))) if metrics.get("5. volume") else None
            )
        except Exception:
            volume_val = None

        row = {
            "symbol": symbol,
            "ts": ts.isoformat(),
            "open": safe_get("1. open"),
            "high": safe_get("2. high"),
            "low": safe_get("3. low"),
            "close": safe_get("4. close"),
            "volume": volume_val,
            "raw": metrics,
        }
        rows.append(row)
    logger.info(f"Parsed {len(rows)} records for {symbol}.")
    return rows


@op
def upsert_postgres(context, rows):
    logger = get_dagster_logger()
    if not rows:
        logger.warning("No rows to upsert.")
        return 0
    conn = None
    try:
        conn = psycopg2.connect(
            host=env("POSTGRES_HOST"),
            port=int(env("POSTGRES_PORT", "5432")),
            user=env("POSTGRES_USER"),
            password=env("POSTGRES_PASSWORD"),
            dbname=env("POSTGRES_DB"),
            connect_timeout=10,
        )
        cur = conn.cursor()
        sql = """
            INSERT INTO market.daily_prices (symbol, ts, open, high, low, close, volume, raw_payload)
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                raw_payload = EXCLUDED.raw_payload
        """
        values = [
            (
                r["symbol"],
                r["ts"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                json.dumps(r["raw"]),
            )
            for r in rows
        ]
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
        logger.info(f"Upserted {len(values)} rows.")
        return len(values)
    except Exception as e:
        logger.error(f"DB error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


@job
def stock_pipeline():
    rows = parse_timeseries(fetch_data())
    upsert_postgres(rows)


def _cron_from_env() -> str:
    interval = os.getenv("FETCH_INTERVAL", "daily").lower()
    if interval == "hourly":
        return "0 * * * *"
    return "0 2 * * *"


stock_schedule = ScheduleDefinition(
    name="stock_schedule",
    job=stock_pipeline,
    cron_schedule=_cron_from_env(),
)

defs = Definitions(
    jobs=[stock_pipeline],
    schedules=[stock_schedule],
)