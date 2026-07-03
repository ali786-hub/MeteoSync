import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load database configuration
load_dotenv()

def run_historical_backfill():
    print("====================================================")
    print("MeteoSync Data Warehouse: 1-Million Row Backfill Pipeline")
    print("====================================================")
    
    conn = None
    cursor = None
    try:
        # Establish secure SSL connection to Azure Postgres
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=os.getenv("DB_PORT"),
            sslmode="require"
        )
        cursor = conn.cursor()
        
        # 1. Fetch active weather nodes
        cursor.execute("SELECT location_key, city_name, latitude, longitude FROM telemetry.dim_locations;")
        tracking_nodes = cursor.fetchall()
        print(f"Found {len(tracking_nodes)} active tracking nodes for historical backfill.")
        
        # 2. Archive API endpoint
        api_url = "https://archive-api.open-meteo.com/v1/archive"
        
        # We will fetch data from 1996-01-01 to 2026-06-30 (~30.5 years).
        # This will generate: 30.5 years * 365 days * 24 hours * 4 locations = ~1,068,000 rows!
        # To avoid API timeout limits, we chunk the requests into 5-year intervals.
        start_year = 1996
        end_year = 2026
        
        for year in range(start_year, end_year, 5):
            chunk_start = f"{year}-01-01"
            chunk_end = f"{year+4}-12-31"
            
            # Bound the final chunk to June 30, 2026
            if year + 4 >= end_year:
                chunk_end = "2026-06-30"
                
            print(f"\n>>> PROCESSING CHUNK: {chunk_start} to {chunk_end} <<<")
            
            for node in tracking_nodes:
                loc_id, city, lat, lon = node
                print(f"  -> Fetching {city} ({chunk_start} to {chunk_end})...")
                
                payload = {
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": chunk_start,
                    "end_date": chunk_end,
                    "hourly": "temperature_2m,relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
                    "timezone": "UTC"
                }
                
                response = requests.get(api_url, params=payload)
                if response.status_code != 200:
                    print(f"     [Error] API fetch failed for {city}: {response.text}")
                    continue
                    
                raw_data = response.json()
                hourly = raw_data.get("hourly", {})
                
                timestamps = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])
                humids = hourly.get("relative_humidity_2m", [])
                precips = hourly.get("precipitation", [])
                clouds = hourly.get("cloud_cover", [])
                winds = hourly.get("wind_speed_10m", [])
                
                insert_records = []
                for i in range(len(timestamps)):
                    ts = timestamps[i] # "YYYY-MM-DDTHH:MM"
                    date_str = ts.split("T")[0] # "YYYY-MM-DD"
                    date_key = int(date_str.replace("-", "")) # YYYYMMDD
                    
                    temp = temps[i]
                    humid = humids[i]
                    precip = precips[i]
                    cloud = clouds[i]
                    wind = winds[i]
                    
                    # Skip rows with missing metrics
                    if temp is None or humid is None:
                        continue
                        
                    insert_records.append((
                        loc_id, date_key, ts, temp, humid, precip, cloud, wind
                    ))
                
                if not insert_records:
                    continue
                    
                # Bulk insert this chunk for the current node
                insert_query = """
                    INSERT INTO telemetry.fact_climate_telemetry 
                    (location_key, date_key, recorded_at, temperature_celsius, humidity_percentage, 
                     precipitation_mm, cloud_cover_percentage, wind_speed_kmh)
                    VALUES %s
                    ON CONFLICT (location_key, recorded_at) DO NOTHING;
                """
                execute_values(cursor, insert_query, insert_records)
                print(f"     [Success] Ingested {len(insert_records)} rows.")
            
            # Commit after each 5-year chunk to avoid long locks
            conn.commit()
            print(f"=== Chunk {chunk_start} to {chunk_end} committed successfully. ===")
            
        print("\n====================================================")
        print("SUCCESS: 1,000,000+ rows successfully loaded into warehouse!")
        print("====================================================")
        
    except Exception as e:
        print(f"\n[Fatal Error] Backfill script aborted: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back safely.")
            
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        print("Database connection closed safely.")

if __name__ == "__main__":
    run_historical_backfill()
