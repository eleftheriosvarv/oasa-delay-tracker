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

def main():
    print("📍 Reading CSV...")
    df = pd.read_csv("stops_selected_lines_renamed.csv")
    grouped = df.groupby("route_code")

    conn = psycopg2.connect(DB_URL)

    for route_code, group in grouped:
        print(f"🚏 Fetching OASA arrivals for route {route_code}...")
        valid = 0
        for stopcode in group["stopcode"]:
            print(f"🔎 Fetching stop={stopcode}, route={route_code}")
            try:
                response = requests.get(
                    f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}",
                    timeout=20,
                )
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and isinstance(data.get("arrivals"), list):
                        arrivals_df = pd.DataFrame(data["arrivals"])
                        if not arrivals_df.empty:
                            arrivals_df["stopcode"] = stopcode
                            arrivals_df["route_code"] = route_code
                            clean_and_store(arrivals_df, conn)
                            valid += 1
            except Exception as e:
                print(f"⚠️ Error for stop {stopcode}, route {route_code}: {e}")
            sleep(1)

        print(f"📊 Total valid rows: {valid}")
        print("🎉 DONE.")

    conn.close()

if __name__ == "__main__":
    main()




