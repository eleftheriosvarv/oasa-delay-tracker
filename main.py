import pandas as pd
import requests
from datetime import datetime
from time import sleep
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

if __name__ == "__main__":
    df_stops = pd.read_csv("a7_2085_stops.csv")
    stopcodes = df_stops["StopCode"].drop_duplicates().astype(str).tolist()
    route_code = "2085"

    results = []

    for stopcode in stopcodes:
        try:
            print(f"Fetching stop {stopcode}...")
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
                            "timestamp": datetime.now(),
                            "stopcode": int(stopcode),
                            "route_code": int(route_code),
                            "veh_code": int(veh_code),
                            "btime2": int(btime2),
                            "delay": None  # to be filled later
                        })
        except Exception as e:
            print(f"Error on stop {stopcode}: {e}")
        sleep(0.2)

    if results:
        df_results = pd.DataFrame(results)

        # Calculate delay
        with engine.connect() as conn:
            for i, row in df_results.iterrows():
                query = text("""
                    SELECT timestamp, btime2 FROM oasa_arrivals
                    WHERE stopcode = :stop AND veh_code = :veh
                    ORDER BY timestamp DESC LIMIT 1
                """)
                prev = conn.execute(query, {"stop": row.stopcode, "veh": row.veh_code}).fetchone()
                if prev:
                    prev_time, prev_btime2 = prev
                    time_diff = (datetime.now() - prev_time).total_seconds() / 60
                    delay = round((prev_btime2 - row.btime2) - time_diff)
                    df_results.at[i, "delay"] = delay

        df_results.to_sql("oasa_arrivals", engine, if_exists="append", index=False)
        print(f"Stored {len(df_results)} records.")
    else:
        print("No arrivals to store.")



