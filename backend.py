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
def get_dashboard_analytics(
    city: str = "Islamabad - Core",
    range: str = Query("168", description="Time range in hours (e.g. 24, 168, 336, 720) or 'all'"),
    start_date: str = Query(None, description="Format: YYYY-MM-DD"),
    end_date: str = Query(None, description="Format: YYYY-MM-DD"),
    year: str = Query(None, description="Specific year (e.g. 2015)")
):
    """Calculates summary KPIs and returns time-series chart data arrays optimized for performance and custom ranges."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Determine filtering mode
            use_custom_filters = bool(start_date or end_date or year)
            
            # Build conditions list
            conditions = ["l.city_name = %s"]
            query_params = [city]
            
            # Determine if we should aggregate daily to save memory
            is_daily_aggregate = False
            limit_hours = 168
            
            if use_custom_filters:
                if year:
                    conditions.append("t.recorded_at >= %s AND t.recorded_at <= %s")
                    query_params.append(f"{year}-01-01 00:00:00")
                    query_params.append(f"{year}-12-31 23:59:59")
                    is_daily_aggregate = True  # A full year is 8,760 hours, aggregate daily to keep charts responsive
                else:
                    if start_date:
                        conditions.append("t.recorded_at >= %s")
                        query_params.append(f"{start_date} 00:00:00")
                    if end_date:
                        conditions.append("t.recorded_at <= %s")
                        query_params.append(f"{end_date} 23:59:59")
                    
                    # Calculate difference to see if we aggregate daily
                    # For simplicity, if both start_date and end_date are provided and the date range is > 45 days, aggregate daily
                    if start_date and end_date:
                        from datetime import datetime
                        try:
                            d1 = datetime.strptime(start_date, "%Y-%m-%d")
                            d2 = datetime.strptime(end_date, "%Y-%m-%d")
                            if (d2 - d1).days > 45:
                                is_daily_aggregate = True
                        except Exception:
                            pass
            else:
                if range == "all":
                    is_daily_aggregate = True
                else:
                    # Relative hours range
                    try:
                        limit_hours = int(range)
                    except ValueError:
                        limit_hours = 168
            
            where_clause = " WHERE " + " AND ".join(conditions)
            
            # Query 1: Calculate summary KPIs matching the filtered dataset
            if use_custom_filters:
                kpi_query = f"""
                    SELECT 
                        MAX(t.temperature_celsius) as max_temp,
                        MIN(t.temperature_celsius) as min_temp,
                        ROUND(AVG(t.humidity_percentage), 2) as avg_humidity,
                        COUNT(t.fact_key) as total_records
                    FROM telemetry.fact_climate_telemetry t
                    JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                    {where_clause};
                """
                cursor.execute(kpi_query, tuple(query_params))
            else:
                # Use standard relative hours query
                if range == "all":
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
                else:
                    kpi_query = """
                        SELECT 
                            MAX(sub.temperature_celsius) as max_temp,
                            MIN(sub.temperature_celsius) as min_temp,
                            ROUND(AVG(sub.humidity_percentage), 2) as avg_humidity,
                            COUNT(sub.fact_key) as total_records
                        FROM (
                            SELECT t.fact_key, t.temperature_celsius, t.humidity_percentage
                            FROM telemetry.fact_climate_telemetry t
                            JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                            WHERE l.city_name = %s
                            ORDER BY t.recorded_at DESC
                            LIMIT %s
                        ) sub;
                    """
                    cursor.execute(kpi_query, (city, limit_hours))
                    
            kpis = cursor.fetchone()
            
            # Handle empty database scenario gracefully
            if not kpis or kpis[3] == 0:
                return {
                    "city": city,
                    "summary": {"max_temp": 0, "min_temp": 0, "avg_humidity": 0, "total_records": 0},
                    "timeline": []
                }
                
            # Query 2: Extract timeline metrics
            if is_daily_aggregate:
                # Group daily
                timeline_query = f"""
                    SELECT 
                        d.full_date,
                        ROUND(AVG(t.temperature_celsius), 2) as avg_temp,
                        ROUND(AVG(t.humidity_percentage), 2) as avg_humid,
                        ROUND(SUM(t.precipitation_mm), 2) as total_precip,
                        ROUND(AVG(t.cloud_cover_percentage), 2) as avg_cloud,
                        ROUND(AVG(t.wind_speed_kmh), 2) as avg_wind,
                        d.day_of_week
                    FROM telemetry.fact_climate_telemetry t
                    JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                    JOIN telemetry.dim_date d ON t.date_key = d.date_key
                    {where_clause}
                    GROUP BY d.full_date, d.day_of_week
                    ORDER BY d.full_date ASC;
                """
                cursor.execute(timeline_query, tuple(query_params))
                rows = cursor.fetchall()
                timeline_data = [
                    {
                        "timestamp": row[0].strftime("%Y-%m-%d"),
                        "temperature": float(row[1]),
                        "humidity": float(row[2]),
                        "precipitation": float(row[3]),
                        "cloud_cover": float(row[4]),
                        "wind_speed": float(row[5]),
                        "day_of_week": row[6]
                    }
                    for row in rows
                ]
            else:
                if use_custom_filters:
                    # Return hourly records for the custom range (no offset, just sorted ascending)
                    timeline_query = f"""
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
                        {where_clause}
                        ORDER BY t.recorded_at ASC;
                    """
                    cursor.execute(timeline_query, tuple(query_params))
                else:
                    # Fetch relative hourly metrics
                    timeline_query = """
                        SELECT * FROM (
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
                            ORDER BY t.recorded_at DESC
                            LIMIT %s
                        ) sub
                        ORDER BY sub.recorded_at ASC;
                    """
                    cursor.execute(timeline_query, (city, limit_hours))
                    
                rows = cursor.fetchall()
                timeline_data = [
                    {
                        "timestamp": row[0].strftime("%Y-%m-%d %H:%M"),
                        "temperature": float(row[1]),
                        "humidity": float(row[2]),
                        "precipitation": float(row[3]),
                        "cloud_cover": float(row[4]),
                        "wind_speed": float(row[5]),
                        "day_of_week": row[6]
                    }
                    for row in rows
                ]
                
            cursor.close()
            
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
    end_date: str = Query(None, description="Format: YYYY-MM-DD"),
    page: int = Query(1, ge=1, description="Page number for pagination"),
    limit: int = Query(50, ge=1, le=100, description="Number of records per page"),
    year: str = Query(None, description="Specific year to filter (e.g. 1996)")
):
    """Provides index-optimized server-side pagination and dynamic filtering for the raw table records view."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Base query conditions
            conditions = ["l.city_name = %s"]
            query_params = [city]
            
            if start_date:
                conditions.append("t.recorded_at >= %s")
                query_params.append(f"{start_date} 00:00:00")
            if end_date:
                conditions.append("t.recorded_at <= %s")
                query_params.append(f"{end_date} 23:59:59")
            if year:
                # Optimized range check to ensure PostgreSQL utilizes indexes on recorded_at
                conditions.append("t.recorded_at >= %s AND t.recorded_at <= %s")
                query_params.append(f"{year}-01-01 00:00:00")
                query_params.append(f"{year}-12-31 23:59:59")
                
            where_clause = " WHERE " + " AND ".join(conditions)
            
            # 1. Fetch total count matching conditions
            count_query = f"""
                SELECT COUNT(t.fact_key)
                FROM telemetry.fact_climate_telemetry t
                JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                {where_clause}
            """
            cursor.execute(count_query, tuple(query_params))
            total_count = cursor.fetchone()[0]
            
            # 2. Fetch the paginated records
            offset = (page - 1) * limit
            records_query = f"""
                SELECT 
                    t.fact_key, l.city_name, t.recorded_at, 
                    t.temperature_celsius, t.humidity_percentage,
                    t.precipitation_mm, t.cloud_cover_percentage, t.wind_speed_kmh
                FROM telemetry.fact_climate_telemetry t
                JOIN telemetry.dim_locations l ON t.location_key = l.location_key
                {where_clause}
                ORDER BY t.recorded_at DESC
                LIMIT %s OFFSET %s;
            """
            # Append limit and offset to parameters
            run_params = list(query_params)
            run_params.extend([limit, offset])
            
            cursor.execute(records_query, tuple(run_params))
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
                
            return {
                "total_count": total_count,
                "page": page,
                "limit": limit,
                "total_pages": (total_count + limit - 1) // limit,
                "records": table_records
            }
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