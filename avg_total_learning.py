import psycopg2
import configparser
import time

## FOR main.py ##
'''
@app.get("/avg_learning")
async def avg_learning():
    return avg_learning(db_state)
'''


# read credentials for aws rds
# parser = configparser.ConfigParser()

# analytical_db_credentials
# parser.read('credentials.conf')
# adb_username = parser.get('analytical_db_credentials', 'username')
# adb_password = parser.get('analytical_db_credentials', 'password')
# adb_host = parser.get('analytical_db_credentials', 'host')
# adb_port = parser.get('analytical_db_credentials', 'port')
# adb_dbname = parser.get('analytical_db_credentials', 'dbname')

# adb_conn = psycopg2.connect(
#         dbname=adb_dbname,
#         user=adb_username,
#         password=adb_password,
#         host=adb_host,
#         port=adb_port,
#     )
# adb_conn.autocommit = True

# print(f"    🔵 Database connections established in {time.time() - start_time:.2f} seconds.")   

start_time = time.time()

# results_list = []

def avg_learning(db_state):
    conn = db_state['conn']
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            country,
            ROUND(AVG(total_learning / 60), 0) AS avg_total_learning
        FROM analytical_responses
        GROUP BY country
        """
    )
    results = cursor.fetchall()
    cursor.close()
    
    country = results[0]
    avg_total_learning = results[1]

    

    # avg_total_learning = int(avg_total_learning) # converts to int (from decimal)
    dict_cnt_hrs = {"country": country, "hours": avg_total_learning}
    # results_list.append(dict_cnt_hrs)
    # total_responses = sum(state['total_rows'] for state in db_state.values())
    # return {"datasets": results_list}
    return {"datasets": dict_cnt_hrs}


## EXAMPLE JSON ##
'''
{
  "datasets": [
    // one of the below object for each country
    {
      "country": "A three-letter country code, uppercased. e.g. GBR",
      "hours": "The average TMINS for this country, as an integer, e.g. 1640"
    }
  ]
}
'''

