import psycopg2
import configparser
import time
import datetime

def submission_time_series(conn,last_maxid, time_series_tracker, time_series_length):
        cursor = conn.cursor()
        cursor.execute(
                    """
                    SELECT COUNT(*), max(id)
                    FROM analytical_responses
                    WHERE id > %s;
                    """,
                    (last_maxid['max_id'],)
                    )
        
        db_data = cursor.fetchall()

        if db_data[0][1]:
                max_id = db_data[0][1]
        else:
                max_id = 0

        submissions = db_data[0][0]

        if last_maxid['max_id'] < max_id :
                last_maxid['max_id'] = max_id

        now = datetime.datetime.now()

        time_string = now.strftime("%H:%M")
        
        time_series_tracker['datasets'][0]['data'].append({"x" : time_string, "y" : submissions})

        if len(time_series_tracker['datasets'][0]['data']) > time_series_length:
            time_series_tracker['datasets'][0]['data'].pop(0)
        
        cursor.close()