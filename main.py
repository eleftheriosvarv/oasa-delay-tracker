import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

df_stops = pd.read_csv("a7_2085_stops.csv")
df_unique = df_stops[['StopCode']].drop_duplicates()
route_code = "2085"

results = []

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

for _, row in df_unique.iterrows():
    stopcode = str(row['StopCode'])

    try:
        url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        data = response.json()

        arrivals = data if isinstance(data, list) else data.get("arrivals", [])
        print(f"Fetched {len(arrivals)} arrivals for stop {stopcode}", flush=True)

        for arrival in arrivals:
            if str(arrival.get("route_code")) == route_code:
                veh_code = arrival.get("veh_code")
                btime2 = arrival.get("btime2")

                if veh_code and btime2 is not None:
                    delay = None
                    query = text("""
                        SELECT timestamp, btime2
                        FROM oasa_arrivals
                        WHERE veh_code = :veh AND stopcode = :stop
                        ORDER BY timestamp DESC
                        LIMIT 1;
                    """)
                    with engine.connect() as conn:
                        prev = conn.execute(query, {"veh": veh_code, "stop": stopcode}).fetchone()
                        if prev:
                            prev_time, prev_btime2 = prev
                            time_diff = (datetime.now() - prev_time).total_seconds() / 60
                            delay = (float(prev_btime2) - float(btime2)) - time_diff

                    results.append({
                        "timestamp": datetime.now(),
                        "stopcode": int(stopcode),
                        "route_code": int(route_code),
                        "veh_code": int(veh_code),
                        "btime2": int(btime2),
                        "delay": round(delay) if delay is not None else None
                    })

    except requests.exceptions.Timeout:
        print(f"Timeout fetching stop {stopcode}", flush=True)
    except Exception as e:
        print(f"Error with stop {stopcode}: {e}", flush=True)

if results:
    df_results = pd.DataFrame(results)
    df_results.to_sql("oasa_arrivals", engine, if_exists="append", index=False)
    print(f"Stored {len(df_results)} records.", flush=True)
else:
    print("No results to store.", flush=True)





