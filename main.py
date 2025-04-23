import pandas as pd
from datetime import datetime
import psycopg2
import os

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
            INSERT INTO

