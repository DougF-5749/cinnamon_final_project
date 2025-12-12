from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.pool import SimpleConnectionPool
from connection_pool import get_adb_conn
import uvicorn
import configparser
from apscheduler.schedulers.background import BackgroundScheduler

from submissions import submission_count
from subs_over_time import subs_over_time_count
from submissions_time import submission_time_series
from avg_total_learning import avg_learning_hours
from escs import esc_scores
from belonging import belonging_scores

# --------------------------- CREDENTIALS -------------------------------- #

parser = configparser.ConfigParser()

parser.read('credentials.conf')

# Forage URL
forage_base_url = parser.get('forage_urls', 'base_url')

# Analytical DB
adb_username = parser.get('analytical_db_credentials', 'username')
adb_password = parser.get('analytical_db_credentials', 'password')
adb_host = parser.get('analytical_db_credentials', 'host')
adb_port = parser.get('analytical_db_credentials', 'port')
adb_dbname = parser.get('analytical_db_credentials', 'dbname')

# --------------------------- FastAPI -------------------------------- #

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[forage_base_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------- IN-MEMORY VARS -------------------------------- #

adb_state = {
    'last_max_id': 0,
    'total_submissions': 0
}

id_tracker = {
    'max_id' : 0
}

time_series_data = {
    "datasets": [
        {
            "id": "Submissions",
            "data": []
        }
    ]
}

MAX_TIME_SERIES_LENGTH = 12
TIME_SERIES_INTERVAL_MINUTES = 15

# ---------------------------- CONN POOL ------------------------------- #

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

print("   🔵 Analytical database connection pool established.\n")

# ---------------------------- SCHEDULER ------------------------------- #

def init_id_tracker(pool, id_tracker: dict):
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM analytical_responses")
        id_tracker['max_id'] = cursor.fetchone()[0]
    finally:
        cursor.close()
        pool.putconn(conn)

def create_scheduler(pool):
    scheduler = BackgroundScheduler()

    def run_time_series_update():
        conn = pool.getconn()
        conn.autocommit = True
        try:
            submission_time_series(
                conn=conn,
                last_maxid=id_tracker,
                time_series_tracker=time_series_data,
                time_series_length=MAX_TIME_SERIES_LENGTH
            )
        finally:
            pool.putconn(conn)

    scheduler.add_job(
        func=run_time_series_update,
        trigger='interval',
        minutes=TIME_SERIES_INTERVAL_MINUTES,
        id='update_time_series'
    )

    # Run once at startup
    run_time_series_update()

    return scheduler

# ---------------------------- ROUTES ------------------------------- #

@app.get("/submissions")
async def submissions(conn = Depends(get_adb_conn)):
    return submission_count(conn, adb_state)

@app.get("/most_popular_submission_times")
async def subs_over_time(conn = Depends(get_adb_conn)):
    return subs_over_time_count(conn)

@app.get("/submissions_over_time")
async def timeseries_data_submissions():
    return time_series_data

@app.get("/avg_learning")
async def avg_learning(conn = Depends(get_adb_conn)):
    return avg_learning_hours(conn)

@app.get("/avg_escs")
async def avg_escs(conn = Depends(get_adb_conn)):
    return esc_scores(conn)

@app.get("/belonging")
async def belonging(conn = Depends(get_adb_conn)):
    return belonging_scores(conn)

# ---------------------------- RUN SCRIPT ------------------------------- #

if __name__ == "__main__":
    init_id_tracker(adb_conn_pool, id_tracker)
    print("🟣 Starting Scheduler Application")
    scheduler = create_scheduler(adb_conn_pool)
    scheduler.start()
    print("   🔵 Scheduler started. Background task is running.\n")

    print("🟣 Starting FastAPI webserver")
    uvicorn.run(app, port=8080)
    print("   🔵 Webserver started. Listening for HTTPS requests...\n")

    print("🔴 Web server stopped. Shutting down scheduler...")
    scheduler.shutdown()
    print("   🔴 Application terminated.\n")