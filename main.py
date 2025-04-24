import pandas as pd
import requests
from datetime import datetime
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
            response = requests.get(url, timeout=10)
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
                            "delay": None
                        })
        except requests.exceptions.Timeout:
            print(f"Timeout while fetching stop {stopcode}")
        except Exception as e:
            print(f"Error on stop {stopcode}: {e}")

    if results:
        df_results = pd.DataFrame(results)

        keys = df_results[["stopcode", "veh_code"]].drop_duplicates()
        key_tuples = [tuple(x) for x in keys.to_numpy()]

        with engine.connect() as conn:
            placeholder = ",".join(["(%s, %s)"] * len(key_tuples))
            flat_values = [v for pair in key_tuples for v in pair]

            query = f'''
                SELECT DISTINCT ON (stopcode, veh_code)
                       stopcode, veh_code, timestamp, btime2
                FROM oasa_arrivals
                WHERE (stopcode, veh_code) IN ({placeholder})
                ORDER BY stopcode, veh_code, timestamp DESC
            '''
            result = conn.execute(text(query), flat_values).fetchall()
            prev_df = pd.DataFrame(result, columns=["stopcode", "veh_code", "prev_timestamp", "prev_btime2"])

        merged = df_results.merge(prev_df, on=["stopcode", "veh_code"], how="left")
        merged["time_diff"] = (datetime.now() - merged["prev_timestamp"]).dt.total_seconds() / 60
        merged["delay"] = (merged["prev_btime2"] - merged["btime2"]) - merged["time_diff"]
        merged["delay"] = merged["delay"].round().astype("Int64")

        final_df = merged.drop(columns=["prev_timestamp", "prev_btime2", "time_diff"])
        final_df.to_sql("oasa_arrivals", engine, if_exists="append", index=False)
        print(f"Stored {len(final_df)} records.")
    else:
        print("No arrivals to store.")



