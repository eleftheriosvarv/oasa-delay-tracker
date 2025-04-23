import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def create_session():
    session = requests.Session()
    retry = Retry(total=0, backoff_factor=0, raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_db_connection():
    db_url = os.environ.get("DB_URL")
    if not db_url:
        raise Exception("DB_URL environment variable not found")
    return psycopg2.connect(db_url)

def fetch_arrivals(session, stopcode, route_code):
    url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
    try:
        response = session.get(url, timeout=20)
        data = response.json()

        if isinstance(data, dict) and isinstance(data.get("arrivals", None), list):
            return [
                {
                    "stopcode": int(stopcode),
                    "route_code": int(route_code),
                    "veh_code": int(arrival["veh_code"]),
                    "btime2": int(arrival["btime2"]),
                    "timestamp": datetime.now()
                }
                for arrival in data["arrivals"]
                if arrival.get("route_code") == str(route_code)
            ]
        return []
    except Exception as e:
        print(f"⚠️ Error for stop {stopcode}: {e}")
        return []

def clean_and_store(data_df, conn):
    data_df = data_df.dropna(subset=["veh_code", "btime2"])
    data_df = data_df.astype({
        "veh_code": "int",
        "btime2": "int",
        "stopcode": "int",
        "route_code": "int"
    })

    for _, row in data_df.iterrows():
        stopcode = row["stopcode"]
        route_code = row["route_code"]
        veh_code = row["veh_code"]
        btime2 = row["btime2"]
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
            delay = round((prev_btime2 - btime2) - time_diff)

        cursor.execute("""
            INSERT INTO oasa_arrivals (timestamp, stopcode, route_code, veh_code, btime2, delay)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (timestamp, stopcode, route_code, veh_code, btime2, delay))

        conn.commit()
        cursor.close()
        print(f"✅ Inserted: stop={stopcode}, veh={veh_code}, delay={delay}")

def main():
    session = create_session()
    df = pd.read_csv("stops_selected_lines_renamed.csv")
    conn = get_db_connection()

    total_valid = 0

    for _, row in df.drop_duplicates(subset=["stopcode", "route_code"]).iterrows():
        stopcode = row["stopcode"]
        route_code = row["route_code"]

        print(f"🔄 Fetching stop={stopcode}, route={route_code}")
        results = fetch_arrivals(session, stopcode, route_code)

        if results:
            clean_and_store(pd.DataFrame(results), conn)
            total_valid += len(results)

    conn.close()
    print(f"📦 Total inserted rows: {total_valid}")
    print("🎉 DONE.")

if __name__ == "__main__":
    main()



