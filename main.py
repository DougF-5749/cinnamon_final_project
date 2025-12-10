from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import uvicorn
from submissions_funcs import init_db_state, submission_count
from db_names import db_names
import configparser

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

print("🟣 Starting analytical database connection")
adb_conn = psycopg2.connect(
    dbname=adb_dbname,
    user=adb_username,
    password=adb_password,
    host=adb_host,
    port=adb_port,
)

adb_conn.autocommit = True
print("   🔵 Analytical database connection established.")

# change this to query ADB
@app.get("/submissions")
async def submissions():
    return submission_count(db_state)

# Create 4 more ednpoints here for the other metrics

if __name__ == "__main__":
    uvicorn.run(app, port=8080)
