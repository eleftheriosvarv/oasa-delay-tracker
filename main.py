import pandas as pd
import requests
import time
import os
from sqlalchemy import create_engine, text

# Database connection
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Read stopcodes from CSV
df_stops = pd.read_csv("a7_2085_stops.csv")
df_unique = df_stops[['StopCode']].drop_duplicates()
route_code = "2085"

results = []

# Set request headers
headers = {
    "User-Agent": "Mozilla/5.0"
}

# Current time rounded to minute
current_time = pd.Timestamp.now().floor('T')

# Collect data
for _, row in df_unique.iterrows():
    stopcode = str(row['StopCode'])

    try:
        url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        arrivals = data if isinstance(data, list) else data.get("arrivals", [])

        print(f"🟡 Stop {stopcode}, Route {route_code} → Arrivals: {len(arrivals)}", flush=True)

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
                            time_diff = (current_time - prev_time).total_seconds() / 60
                            btime2_diff = float(prev_btime2) - float(btime2)
                            delay = time_diff - btime2_diff

                    results.append({
                        "timestamp": current_time,
                        "stopcode": stopcode,
                        "route_code": route_code,
                        "veh_code": veh_code,
                        "btime2": btime2,
                        "delay": delay
                    })

    except Exception as e:
        print(f"❌ Error for stop {stopcode}: {e}")

    time.sleep(0.2)

# Save results to PostgreSQL
df_results = pd.DataFrame(results)
df_results.to_sql("oasa_arrivals", engine, if_exists="append", index=False)
print(f"✅ Saved {len(df_results)} records.")
