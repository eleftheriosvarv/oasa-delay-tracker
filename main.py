import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
import os
from datetime import datetime

# Only the 5 reliable stopcodes for route 2085
STOPCODES = [10374, 61011, 60985, 60010, 60011]
ROUTE_CODE = 2085

def create_session():
    session = requests.Session()
    retry = Retry(
        total=0,  # no retries
        backoff_factor=0,
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_db_connection():
    db_url = os.environ.get("DB_URL")
    if not db_url:
        raise Exception("DB_URL environment variable not found")
    return psycopg2.connect(db_url)

def fetch_arrivals(session, stopcode):
    url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
    try:
        response = session.get(url, timeout=5)
        data = response.json()

        if isinstance(data, dict) and isinstance(data.get("arrivals", None), list):
            return [
                {
                    "stopcode": stopcode,
                    "route_code": ROUTE_CODE,
                    "veh_code": int(arrival["veh_code"]),
                    "btime2": int(arrival["btime2"]),
                    "timestamp": datetime.now()
                }
                for arrival in data["arrivals"]
                if arrival.get("route_code") == str(ROUTE_CODE)
            ]
        return []
    except Exception as e:
        print(f"⚠️ Error for stop {stopcode}: {e}")
        return []

def clean_and_store(data_df, conn):
    data_df = data_df.dropna(subset=["veh_code", "btime2"])
    data_df["veh_code"] = data_df["veh_code"].astype(int)
    data_df["btime2"] = data_df["btime2"].astype(int)

    for _, row in data_df.iterrows():
        stopcode = int(row["stopcode"])
        route_code = int(row["route_code"])
        veh_code = int(row["veh_code"])
        btime2 = int(row["btime2"])
        timestamp = row["timestamp"]

        cursor = conn.cursor()

        cursor.execute("""
            SELECT timestamp, btime2 FROM oasa_arrivals
            WHERE stopcode = %s AND veh_code = %s
            ORDER BY timestamp DESC LIMIT 1
        """, (stopcode, veh_code))
        prev = cursor.fetchone()

        delay = None
        if prev:
            prev_time, prev_btime2 = prev
            time_diff = (timestamp - prev_time).total_seconds() / 60
            delay = (prev_btime2 - btime2) - time_diff
            delay = round(delay)

        cursor.execute("""
            INSERT INTO oasa_arrivals (timestamp, stopcode, route_code, veh_code, btime2, delay)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (timestamp, stopcode, route_code, veh_code, btime2, delay))

        conn.commit()
        print(f"✅ Inserted: stop={stopcode}, veh={veh_code}, delay={delay}")

def main():
    session = create_session()
    all_results = []

    print("🚀 Fetching OASA arrivals for route 2085...")

    for stopcode in STOPCODES:
        print(f"🔍 Fetching stop={stopcode}")
        results = fetch_arrivals(session, stopcode)
        all_results.extend(results)

    print(f"📦 Total valid rows: {len(all_results)}")

    if all_results:
        conn = get_db_connection()
        clean_and_store(pd.DataFrame(all_results), conn)
        conn.close()

    print("🎉 DONE.")

if __name__ == "__main__":
    main()


