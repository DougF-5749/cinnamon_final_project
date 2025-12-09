import psycopg2
import configparser

# read credentials for aws rds
parser = configparser.ConfigParser()
parser.read('credentials.conf')
username = parser.get('source_aws_rds_credentials', 'username')
password = parser.get('source_aws_rds_credentials', 'password')
host_start = parser.get('source_aws_rds_credentials', 'host_start')
host_last = parser.get('source_aws_rds_credentials', 'host_last')
port = parser.get('source_aws_rds_credentials', 'port')

def init_db_state(db_names: list[str], db_state: dict = {}) -> dict[str, dict]:
    for db_name in db_names:
        host = f"{host_start}{db_name}{host_last}"
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port,
        )
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COALESCE(MAX(id), 0) AS last_max_id,
                COUNT(*) AS baseline_count
            FROM responses;
            """
        )
        last_max_id, baseline_count = cur.fetchone()
        cur.close()

        db_state[db_name] = {
            "conn": conn,
            "last_max_id": last_max_id,
            "total_rows": baseline_count,
        }
    return db_state

def submission_count(db_state: dict[str, dict]) -> dict:
    new_responses = 0

    for state in db_state.values():
            conn = state['conn']
            last_max = state['last_max_id'] 

            cursor = conn.cursor() 
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS new_rows,
                    COALESCE(MAX(id), %s) AS new_max_id
                FROM responses
                WHERE id > %s;
                """, (last_max, last_max),
            )

            new_rows, new_max_id = cursor.fetchone()
            cursor.close()

            if new_rows > 0:
                state['last_max_id'] = new_max_id
                state['total_rows'] += new_rows
                new_responses += new_rows

    total_responses = sum(state['total_rows'] for state in db_state.values())

    return {"count": total_responses}


