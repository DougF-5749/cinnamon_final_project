from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.pool import SimpleConnectionPool
import uvicorn
from submissions_funcs import submission_count
from db_names import db_names
import configparser

# function to abstract away getting a connection from the pool
def get_adb_conn():
    conn = adb_conn_pool.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        adb_conn_pool.putconn(conn)
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

# change this to query ADB
@app.get("/submissions")
async def submissions(conn = Depends(get_adb_conn)):
    return submission_count(conn)

# @app.get("/endpoint-2")
# async def endpoint_2():
#     return endpoint_2_func(adb_conn_pool.getconn())

@app.get("/avg_learning")
async def avg_learning(conn = Depends(get_adb_conn)):
    return avg_learning(conn)

# @app.get("/endpoint-4")
# async def endpoint_4():
#     return endpoint_4_func(adb_conn_pool.getconn())

# @app.get("/endpoint-5")
# async def endpoint_5():
#     return endpoint_5_func(adb_conn_pool.getconn())

# Create 4 more ednpoints here for the other metrics

if __name__ == "__main__":
    uvicorn.run(app, port=8080)
