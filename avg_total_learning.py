import psycopg2
import configparser
import time

## FOR main.py ##
'''
@app.get("/avg_learning")
async def avg_learning():
    return avg_learning(db_state)
'''


def avg_learning(conn):
    # conn = db_state['conn']
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

    results_list = []

    for country_data in results:
        country = country_data[0]
        avg_total_learning = country_data[1]

        dict_cnt_hrs = {"country": country, "hours": avg_total_learning}
        results_list.append(dict_cnt_hrs)
    
    print(f'Results list: {results_list}')

    return {"datasets": results_list}

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

