from time import time
import psycopg2
import configparser
from db_names import db_names

# source_aws_rds_credentials
parser = configparser.ConfigParser()
parser.read('credentials.conf')
src_username = parser.get('source_aws_rds_credentials', 'username')
src_password = parser.get('source_aws_rds_credentials', 'password')
src_host_start = parser.get('source_aws_rds_credentials', 'host_start')
src_host_last = parser.get('source_aws_rds_credentials', 'host_last')
src_port = parser.get('source_aws_rds_credentials', 'port')

# analytical_db_credentials
parser.read('credentials.conf')
adb_username = parser.get('analytical_db_credentials', 'username')
adb_password = parser.get('analytical_db_credentials', 'password')
adb_host = parser.get('analytical_db_credentials', 'host')
adb_port = parser.get('analytical_db_credentials', 'port')
adb_dbname = parser.get('analytical_db_credentials', 'dbname')

source_db_state = {} # to hold source db connections

new_submissions_list = [] # to hold new submissions before inserting into analytical_db

POLLING_TIME = 30 # seconds

try:
    for db_name in db_names:
        host = f"{src_host_start}{db_name}{src_host_last}"
        conn = psycopg2.connect(
            dbname=db_name,
            user=src_username,
            password=src_password,
            host=host,
            port=src_port,
        )
        conn.autocommit = True

        source_db_state[db_name] = {
            "conn": conn,
        }

    adb_conn = psycopg2.connect(
        dbname=adb_dbname,
        user=adb_username,
        password=adb_password,
        host=adb_host,
        port=adb_port,
    )
    adb_conn.autocommit = True    

    while True:
        for state in source_db_state.values():
            conn = state['conn']

            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    id AS initial_id,
                    belong,
                    durecec,
                    tmins,
                    escs,
                    cnt
                FROM responses;
                """
            )
            new_submissions = cur.fetchall() 
            cur.close()

            # validating / cleaning data before insertion 
            for row in new_submissions:
                modified = list(row)
                for i in range(len(modified)):
                    if modified[i] == 'NA':
                        modified[i] = None
                modified[0] = int(modified[0])  # initial_id
                modified[1] = float(modified[1]) if modified[1] is not None else None  # belonging
                modified[2] = int(modified[2]) if modified[2] is not None else None  # ec_yrs
                modified[3] = int(modified[3]) if modified[3] is not None else None  # total_learning_mins
                modified[4] = float(modified[4]) if modified[4] is not None else None  # esc_score
                modified[5] = str(modified[5]) if modified[5] is not None else None  # country
                                    
                new_submissions_list.append(modified)

        # insert new submissions into analytical_db
        for submission in new_submissions_list:
            cur = adb_conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytical_responses (
                    id SERIAL PRIMARY KEY,
                    initial_id INTEGER,
                    belonging FLOAT,
                    early_ed_years INTEGER,
                    total_learning INTEGER,
                    econ_soc_cul_status FLOAT,
                    country VARCHAR(3),
                    datetime_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO analytical_responses (
                    initial_id,
                    belonging,
                    early_ed_years,
                    total_learning,
                    econ_soc_cul_status,
                    country
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (initial_id) DO NOTHING;
                """,
                submission[0],  # initial_id
                submission[1],  # belonging
                submission[2],  # ec_yrs
                submission[3],  # total_learning_mins
                submission[4],  # esc_score
                submission[5],  # country
            )
            cur.close()
        new_submissions_list.clear()
        time.sleep(POLLING_TIME)

except KeyboardInterrupt:
    print("🟣 User interrupted the process.")
    for db_name, state in source_db_state.items():
        state['conn'].close()
        print(f"    Connection to {db_name} closed.")
    print("🟣 Database connections closed.")
