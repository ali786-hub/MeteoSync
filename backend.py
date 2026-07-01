import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import hashlib
from dotenv import load_dotenv

# Load database environment variables
load_dotenv()

app = FastAPI(
    title="MeteoSync Enterprise Gateway",
    description="Production-ready backend architecture serving analytics, raw exploration, and secure gateway access."
)

# Enable CORS so local HTML/JS files can hit this API seamlessly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 1. DATABASE CONNECTION POOLING
# ==========================================
# Creates a pool of warm connections to eliminate handshake latency on every request
try:
    db_pool = ThreadedConnectionPool(
        1, 10,  # Maintain between 1 and 10 concurrent connections
        host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), database=os.getenv("DB_NAME"), port=os.getenv("DB_PORT"),
        sslmode="require"
    )
    print("Database Connection Pool initialized successfully.")
except Exception as e:
    print(f"Error initializing connection pool: {e}")
    db_pool = None

@contextmanager
def get_db_connection():
    """Yields a database connection from the pool and automatically returns it."""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized.")
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)


# ==========================================
# 2. PASSWORD HASHING UTILITIES
# ==========================================
def hash_password(password: str) -> str:
    """Hashes a password using PBKDF2 (Cryptographically secure)."""
    salt = b"meteosync_enterprise_secure_salt_string"
    key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return key.hex()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain-text password against the stored hash."""
    return hash_password(plain_password) == hashed_password


# ==========================================
# 3. DATA MODELS (PYDANTIC)
# ==========================================
class UserAuth(BaseModel):
    username: str
    password: str

class LocationCreate(BaseModel):
    city_name: str
    latitude: float
    longitude: float


# ==========================================
# MODULE 1: AUTHENTICATION SYSTEM GATEWAY
# ==========================================

@app.post("/api/auth/signup")
def user_signup(user: UserAuth):
    """Registers a new system user inside the isolated 'auth' schema."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if the username already exists
            cursor.execute("SELECT user_key FROM auth.dim_users WHERE username = %s;", (user.username,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Username is already taken.")
                
            # Insert the fresh user record with a SECURE PASSWORD HASH
            hashed_pw = hash_password(user.password)
            cursor.execute(
                "INSERT INTO auth.dim_users (username, password_hash, role) VALUES (%s, %s, 'viewer');",
                (user.username, hashed_pw)
            )
            conn.commit()
            cursor.close()
            return {"status": "success", "message": "User registration completed successfully."}
            
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database failure during signup: {str(e)}")

@app.post("/api/auth/login")
def user_login(user: UserAuth):
    """Validates user credentials and grants entry into the platform."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Extract the account hash to verify against the submitted password
            cursor.execute(
                "SELECT user_key, username, role, password_hash FROM auth.dim_users WHERE username = %s;",
                (user.username,)
            )
            account = cursor.fetchone()
            cursor.close()
            
            if not account or not verify_password(user.password, account[3]):
                raise HTTPException(status_code=401, detail="Invalid username or password.")
                
            return {
                "status": "authenticated",
                "user_id": account[0],
                "username": account[1],
                "role": account[2]
            }
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database login fault: {str(e)}")


# ==========================================
# MODULE 2: ANALYTICS DASHBOARD API
# ==========================================

@app.get("/api/analytics/dashboard")
def get_dashboard_analytics(city: str = "Islamabad - Core"):
    """Calculates summary KPIs and returns time-series chart data arrays."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Query 1: Calculate high-level summary metadata (KPI box cards)
            kpi_query = """
                SELECT 
                    MAX(temperature_celsius) as max_temp,
                    MIN(temperature_celsius) as min_temp,
                    ROUND(AVG(humidity_percentage), 2) as avg_humidity,
                    COUNT(fact_key) as total_records
                FROM telemetry.fact_climate_telemetry t
                JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                WHERE l.city_name = %s;
            """
            cursor.execute(kpi_query, (city,))
            kpis = cursor.fetchone()
            
            # Handle empty database scenario gracefully
            if kpis[3] == 0:
                return {
                    "city": city,
                    "summary": {"max_temp": 0, "min_temp": 0, "avg_humidity": 0, "total_records": 0},
                    "timeline": []
                }
                
            # Query 2: Extract ALL 5 METRICS for the frontend charting engine
            # We also join with our dim_date table to return day_of_week context!
            timeline_query = """
                SELECT 
                    t.recorded_at, 
                    t.temperature_celsius, 
                    t.humidity_percentage,
                    t.precipitation_mm,
                    t.cloud_cover_percentage,
                    t.wind_speed_kmh,
                    d.day_of_week
                FROM telemetry.fact_climate_telemetry t
                JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                JOIN telemetry.dim_date d ON t.date_key = d.date_key
                WHERE l.city_name = %s
                ORDER BY t.recorded_at ASC;
            """
            cursor.execute(timeline_query, (city,))
            rows = cursor.fetchall()
            cursor.close()
            
            # Structure time-series list
            timeline_data = []
            for row in rows:
                timeline_data.append({
                    "timestamp": row[0].strftime("%Y-%m-%d %H:%M"),
                    "temperature": float(row[1]),
                    "humidity": float(row[2]),
                    "precipitation": float(row[3]),
                    "cloud_cover": float(row[4]),
                    "wind_speed": float(row[5]),
                    "day_of_week": row[6]
                })
                
            return {
                "city": city,
                "summary": {
                    "max_temp": float(kpis[0]) if kpis[0] else 0,
                    "min_temp": float(kpis[1]) if kpis[1] else 0,
                    "avg_humidity": float(kpis[2]) if kpis[2] else 0,
                    "total_records": kpis[3]
                },
                "timeline": timeline_data
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics computation fault: {str(e)}")


# ==========================================
# MODULE 3: RAW DATA EXPLORER & ENGINE FILTERS API
# ==========================================

@app.get("/api/explorer/data")
def explore_raw_data(
    city: str = "Islamabad - Core", 
    start_date: str = Query(None, description="Format: YYYY-MM-DD"),
    end_date: str = Query(None, description="Format: YYYY-MM-DD")
):
    """Provides server-side dynamic filtering for the raw table records view."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Build dynamic relational filtering base query targeting the Star Schema
            base_query = """
                SELECT 
                    t.fact_key, l.city_name, t.recorded_at, 
                    t.temperature_celsius, t.humidity_percentage,
                    t.precipitation_mm, t.cloud_cover_percentage, t.wind_speed_kmh
                FROM telemetry.fact_climate_telemetry t
                JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                WHERE l.city_name = %s
            """
            query_params = [city]
            
            # Append runtime date string parameters dynamically
            if start_date:
                base_query += " AND t.recorded_at >= %s"
                query_params.append(f"{start_date} 00:00:00")
            if end_date:
                base_query += " AND t.recorded_at <= %s"
                query_params.append(f"{end_date} 23:59:59")
                
            base_query += " ORDER BY t.recorded_at DESC;"
            
            cursor.execute(base_query, tuple(query_params))
            rows = cursor.fetchall()
            cursor.close()
            
            table_records = []
            for row in rows:
                table_records.append({
                    "record_id": row[0],
                    "city_name": row[1],
                    "recorded_at": row[2].strftime("%Y-%m-%d %H:%M"),
                    "temperature": float(row[3]),
                    "humidity": float(row[4]),
                    "precipitation": float(row[5]),
                    "cloud_cover": float(row[6]),
                    "wind_speed": float(row[7])
                })
                
            return {"records_count": len(table_records), "records": table_records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Explorer filtration failure: {str(e)}")


# ==========================================
# MODULE 4: ADMINISTRATIVE PANEL ROUTE
# ==========================================

@app.post("/api/admin/locations")
def add_new_monitored_location(loc: LocationCreate):
    """Enables admin accounts to inject new geographic node fields directly."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO telemetry.dim_locations (city_name, latitude, longitude) VALUES (%s, %s, %s) "
                "ON CONFLICT (latitude, longitude) DO NOTHING;",
                (loc.city_name, loc.latitude, loc.longitude)
            )
            conn.commit()
            cursor.close()
            return {"status": "success", "message": f"New location tracking node '{loc.city_name}' deployed successfully."}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Admin modification rejected: {str(e)}")