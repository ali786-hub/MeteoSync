import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_enterprise_pipeline():
    print("Initiating MeteoSync ETL Pipeline...")
    
    conn = None
    cursor = None
    try:
        # 1. Connect to Azure PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"), 
            database=os.getenv("DB_NAME"), 
            port=os.getenv("DB_PORT"),
            sslmode="require"
        )
        cursor = conn.cursor()
        
        # 2. Dynamically fetch ALL active tracking nodes from the 'telemetry' schema
        cursor.execute("SELECT location_key, city_name, latitude, longitude FROM telemetry.dim_locations;")
        tracking_nodes = cursor.fetchall()
        print(f"Discovered {len(tracking_nodes)} active microclimate nodes in database.")
        
        # Base API configuration for Open-Meteo
        api_url = "https://api.open-meteo.com/v1/forecast"
        
        # 3. Loop through every microclimate and extract data
        for node in tracking_nodes:
            loc_id, city, lat, lon = node
            print(f"\nTargeting Open-Meteo for: {city} (Lat: {lat}, Lon: {lon})")
            
            payload = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
                "past_days": 1,  
                "forecast_days": 0,
                "timezone": "UTC"
            }
            
            response = requests.get(api_url, params=payload)
            if response.status_code != 200:
                print(f"API fault for {city}. Skipping.")
                continue
                
            raw_data = response.json()
            hourly = raw_data.get("hourly", {})
            
            timestamps = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humids = hourly.get("relative_humidity_2m", [])
            precips = hourly.get("precipitation", [])
            clouds = hourly.get("cloud_cover", [])
            winds = hourly.get("wind_speed_10m", [])
            
            # 4. Transform raw JSON arrays into relational tuples for batch insertion
            insert_records = []
            for i in range(len(timestamps)):
                # Calculate the date_key for our dim_date table (e.g., "2024-05-12T12:00" -> 20240512)
                date_string = timestamps[i][:10] # extracts "YYYY-MM-DD"
                date_key = int(date_string.replace('-', ''))
                
                # Handle potential nulls from the API
                temp = temps[i] if temps[i] is not None else 0.0
                humid = humids[i] if humids[i] is not None else 0.0
                precip = precips[i] if precips[i] is not None else 0.0
                cloud = clouds[i] if clouds[i] is not None else 0.0
                wind = winds[i] if winds[i] is not None else 0.0

                # Form the tuple row matching the fact_climate_telemetry columns
                record = (
                    loc_id, date_key, timestamps[i], temp, humid,
                    precip, cloud, wind
                )
                insert_records.append(record)
                
            # 5. Load data with Batch Insertion (execute_values) and Idempotency (ON CONFLICT)
            insert_query = """
                INSERT INTO telemetry.fact_climate_telemetry 
                (location_key, date_key, recorded_at, temperature_celsius, humidity_percentage, 
                 precipitation_mm, cloud_cover_percentage, wind_speed_kmh)
                VALUES %s
                ON CONFLICT (location_key, recorded_at) DO UPDATE SET
                    temperature_celsius = EXCLUDED.temperature_celsius,
                    humidity_percentage = EXCLUDED.humidity_percentage,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    cloud_cover_percentage = EXCLUDED.cloud_cover_percentage,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    inserted_at = CURRENT_TIMESTAMP;
            """
            
            # psycopg2.extras.execute_values is highly optimized for bulk inserts
            execute_values(cursor, insert_query, insert_records)
            print(f"Successfully batch-processed and upserted {len(timestamps)} metric arrays for {city}.")
            
        # Commit all transactions at the end of the pipeline
        conn.commit()
        print("\nSUCCESS: Global ETL synchronization complete.")
        
    except Exception as e:
        print(f"Pipeline Aborted: {e}")
        if conn:
            conn.rollback() # Ensure partial data isn't saved if the script crashes
            
    finally:
        # Securely close connections to prevent database locking
        if cursor: cursor.close()
        if conn: conn.close()
        print("Database connection closed safely.")

if __name__ == "__main__":
    run_enterprise_pipeline()
