import pandas as pd
import requests
import psycopg2
from datetime import datetime
from urllib.parse import urlparse
import os
import time

def fetch_arrivals(input_file):
    df = pd.read_csv(input_file)
    df_unique = df[['stopcode', 'route_code']].drop_duplicates()

    results = []

    for _, row in df_unique.iterrows():
        stopcode = str(row['stopcode'])
        route_code = str(row['route_code'])

        try:
            url = f"https://telematics.oasa.gr/api/?act=getStopArrivals&p1={stopcode}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            arrivals = data if isinstance(data, list) else data.get("arrivals", [])

            for arrival in arrivals:
                if str(arrival.get("route_code")) == route_code:
                    results.append({
                        "stopcode": stopcode,
                        "route_code": route_code,
                        "veh_code": arrival.get("veh_code"),
                        "btime2": arrival.get("btime2")
                    })

        except Exception as e:
            print(f"Error fetching {stopcode}/{route_code}: {e}")

        time.sleep(0.4)

    return pd.DataFrame(results)

def clean_and_store(data_df, db_connection):
    now = datetime.now()
    data_df = data_df.dropna(subset=['veh_code', 'btime2'])
    data_df['veh_code'] = data_df['veh_code'].astype(int)
    data_df['btime2'] = data_df['btime2'].astype(int)

    for _, row in data_df.iterrows():
        stopcode = int(row['stopcode'])
        route_code = int(row['route_code'])
        veh_code = int(row['veh_code'])
        btime2 = int(row['btime2'])

        cursor = db_connection.cursor()

        cursor.execute("""
            SELECT timestamp, btime2 FROM oasa_arrivals
            WHERE stopcode = %s AND veh_code = %s
            ORDER BY timestamp DESC LIMIT 1
        """, (stopcode, veh_code))
        prev = cursor.fetchone()

        delay = None
        if prev:
            prev_time, prev_btime2 = prev
            time_diff = (now - prev_time).total_seconds() / 60
            delay = (prev_btime2 - btime2) - time_diff
            delay = round(delay)

        cursor.execute("""
            INSERT INTO oasa_arrivals (timestamp, stopcode, route_code, veh_code, btime2, delay)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (now, stopcode, route_code, veh_code, btime2, delay))

        db_connection.commit()

def main():
    # Get DB connection
    db_url = os.environ["DB_URL"]
    result = urlparse(db_url)
    conn = psycopg2.connect(
        host=result.hostname,
        port=result.port,
        dbname=result.path.lstrip('/'),
        user=result.username,
        password=result.password
    )

    # Fetch live data
    df = fetch_arrivals("stops_selected_lines_renamed.csv")

    # Clean and store in DB
    clean_and_store(df, conn)

    conn.close()

if __name__ == "__main__":
    main()


