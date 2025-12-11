def esc_scores(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            country,
            AVG(econ_soc_cul_status) * 4
        FROM analytical_responses
        GROUP BY country
        """
    )

    results = cursor.fetchall()
    cursor.close()

    results_list = []

    for country_data in results:
        country = country_data[0]
        avg_escs = country_data[1]

        dict_cnt_escs = {"id": country, "value": avg_escs}
        results_list.append(dict_cnt_escs)

    return {"datasets": results_list}

## EXAMPLE JSON ##
'''
{
  "datasets": [
    // one of the below object for each country
    {
      "id": "A three-letter country code, uppercased. e.g. GBR",
      "value": "The country's ESCS score, usually between -5.0 and 5.0."
    }
  ]
}
'''

