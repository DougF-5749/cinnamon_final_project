from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from submissions_funcs import init_db_state, submission_count
from db_names import db_names
import configparser

parser = configparser.ConfigParser()
parser.read('credentials.conf')
forage_base_url = parser.get('forage_urls', 'base_url')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[forage_base_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_state = init_db_state(db_names)

print("Initial database states:")
for name, state in db_state.items():
    print(f"📊 {name}: baseline_rows={state['total_rows']}")

@app.get("/submissions")
async def submissions():
    return submission_count(db_state)

if __name__ == "__main__":
    uvicorn.run(app, port=8080)
