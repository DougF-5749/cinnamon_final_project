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
                COUNT(*) AS baseline_count
            FROM responses;
            """
        )
        baseline_count = cur.fetchone()[0]
        cur.close()

        db_state[db_name] = {
            "conn": conn,
            "total_rows": baseline_count,
        }
    return db_state

def submission_count(db_state: dict[str, dict]) -> dict:
    for state in db_state.values():
            conn = state['conn']

            cursor = conn.cursor() 
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS updated_row_count
                FROM responses
                """
            )

            updated_row_count= cursor.fetchone()[0]
            cursor.close()

            if updated_row_count > state['total_rows']:
                state['total_rows'] += updated_row_count

    total_responses = sum(state['total_rows'] for state in db_state.values())

    return {"count": total_responses}


