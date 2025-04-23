import pandas as pd
import requests
import psycopg2
from datetime import datetime
from time import sleep
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")

def clean_and_store(data_df, db_connection):
    now = datetime.now()
    data_df = data_df.dropna(subset=["veh_code", "btime2"])
    data_df["veh_code"] = data_df["veh_code"].astype(int)
    data_df["btime2"] = data_df["btime2"].astype(int)

    for _, row in data_df.iterrows():
        stopcode = int(row["stopcode"])
        route_code = int(row["route_code"])
        veh_code = int(row["veh_code"])
        btime2 = int(row["btime2"])

        cursor = db_connection.cursor()

        cursor.execute(
            """
            SELECT timestamp, btime2 FROM oasa_arrivals
            WHERE stopcode = %s AND veh_code = %s
            ORDER BY timestamp DESC LIMIT 1
            """,
            (stopcode, veh_code),
        )
        prev = cursor.fetchone()

        delay = None
        if prev:
            prev_time, prev_btime2 = prev
            time_diff = (now - prev_time).total_seconds() / 60
            delay = round((prev_btime2 - btime2) - time_diff)

        cursor.execute(
            """
            INSERT INTO oasa_arrivals (timestamp, stopcode, route_code, veh_code, btime2, delay)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, stopcode, veh_code) DO NOTHING
            """,
            (now, stopcode, route_code, veh_code, btime2, delay),
        )

        db_connection.commit()
        cursor.close()

if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)

    df_stops = pd.read_csv("a7_2085_stops.csv")
    stopcodes = df_stops["StopCode"].drop_duplicates().astype(str).tolist()
    route_code = "2085"

    results = []

    for stopcode in stopcodes:
        try:
            url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            arrivals = data if isinstance(data, list) else data.get("arrivals", [])

            for arrival in arrivals:
                if str(arrival.get("route_code")) == route_code:
                    veh_code = arrival.get("veh_code")
                    btime2 = arrival.get("btime2")
                    if veh_code and btime2:
                        results.append({
                            "stopcode": int(stopcode),
                            "route_code": int(route_code),
                            "veh_code": int(veh_code),
                            "btime2": int(btime2)
                        })
        except Exception as e:
            print(f"❌ Error for stop {stopcode}: {e}")
        sleep(0.2)

    if results:
        df_results = pd.DataFrame(results)
        clean_and_store(df_results, conn)
        print(f"✅ Stored {len(df_results)} records.")
    else:
        print("⚠️ No results to store.")

    conn.close()





