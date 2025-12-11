from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.pool import SimpleConnectionPool
import uvicorn
from belonging import belonging_scores
from submissions_time import submission_time_series
from subs_over_time import subs_over_time_count
from escs import esc_scores
from submissions_funcs import submission_count
from avg_total_learning import avg_learning_hours
from db_names import db_names
import configparser
from apscheduler.schedulers.background import BackgroundScheduler


# function to abstract away getting a connection from the pool
def get_adb_conn():
    conn = adb_conn_pool.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        adb_conn_pool.putconn(conn)

id_tracker = {
    'max_id' : 0
}
time_series_data = {"datasets": [{"id": "Submissions",
      "data": []}]}

MAX_TIME_SERIES_LENGTH = 12
# -----------------------------------------------------------

parser = configparser.ConfigParser()
parser.read('credentials.conf')
forage_base_url = parser.get('forage_urls', 'base_url')

# analytical_db_credentials
parser.read('credentials.conf')
adb_username = parser.get('analytical_db_credentials', 'username')
adb_password = parser.get('analytical_db_credentials', 'password')
adb_host = parser.get('analytical_db_credentials', 'host')
adb_port = parser.get('analytical_db_credentials', 'port')
adb_dbname = parser.get('analytical_db_credentials', 'dbname')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[forage_base_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("🟣 Creating analytical database connection pool")
adb_conn_pool = SimpleConnectionPool(
    minconn=5,
    maxconn=10,
    dbname=adb_dbname,
    user=adb_username,
    password=adb_password,
    host=adb_host,
    port=adb_port
)

print("   🔵 Analytical database connection pool established.")

adb_state = {
    'last_max_id': 0,
    'total_submissions': 0
}

# change this to query ADB
@app.get("/submissions")
async def submissions(conn = Depends(get_adb_conn)):
    return submission_count(conn, adb_state)

@app.get("/subs_over_time")
async def subs_over_time(conn = Depends(get_adb_conn)):
    return subs_over_time_count(conn)

@app.get("/avg_learning")
async def avg_learning(conn = Depends(get_adb_conn)):
    return avg_learning_hours(conn)

@app.get("/avg_escs")
async def avg_escs(conn = Depends(get_adb_conn)):
    return esc_scores(conn)

@app.get("/belonging")
async def belonging(conn = Depends(get_adb_conn)):
    return belonging_scores(conn)

@app.get("/submissions_over_time")
async def timeseries_data_submissions():
    return time_series_data

# Create 4 more ednpoints here for the other metrics

if __name__ == "__main__":
    conn = adb_conn_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM analytical_responses")
    id_tracker['max_id'] = cursor.fetchone()[0]
    cursor.close()
    adb_conn_pool.putconn(conn)
    # 1. Setup and Start the Scheduler FIRST
    print("--- Starting Scheduler Application ---")
    scheduler = BackgroundScheduler()
    
    # Create a wrapper function that gets a real connection
    def run_time_series_update():
        conn = adb_conn_pool.getconn()  # Get actual connection from pool
        conn.autocommit = True
        try:
            submission_time_series(conn=conn, last_maxid=id_tracker, time_series_tracker=time_series_data, time_series_length=MAX_TIME_SERIES_LENGTH)
        finally:
            adb_conn_pool.putconn(conn)  # Return connection to pool
    
    scheduler.add_job(
        func=run_time_series_update,  # Use the wrapper function
        trigger='interval',
        minutes=1,
        id='update_time_series'
    )
    
    # Run once immediately
    run_time_series_update()
    scheduler.start()
    print("Scheduler started. Background task is running.")
    
    # 2. Start the Blocking Web Server
    uvicorn.run(app, port=8080)
    
    # 3. Handle shutdown
    print("\nWeb server stopped. Shutting down scheduler...")
    scheduler.shutdown() 
    print("Application terminated.")