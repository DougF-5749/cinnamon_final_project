def belonging_scores(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
	        country,
	        AVG(early_ed_years),
	        AVG(belonging),
	        COUNT(*)
        FROM
	        analytical_responses
        GROUP BY
	        country;
        """
    )

    results = cursor.fetchall()
    cursor.close()

    results_list = []

    for country_data in results:
        country = country_data[0]
        avg_durecec = country_data[1]
        avg_belonging = country_data[2]
        count = country_data[3]

        dict_cnt_escs = {"id": country, 
                         "data": [
            {
                "x": int(avg_durecec),
                "y": float(avg_belonging),
                "submissions": int(count)
            }
        ]}
        results_list.append(dict_cnt_escs)

    return {"datasets": results_list}

## EXAMPLE JSON ##
'''
{
  "datasets": [
    // one of the below object for each country
    {
      "id": "A three-letter country code, uppercased. e.g. GBR",
      "data": [
                {
                  "x": "DURECEC as an integer, e.g. 6",
                  "y": "BELONG as a float, e.g. 1.1",
                  "submissions": "a count of submissions from this country as an integer, e.g. 412"
                }
              ]
    }
  ]
}
'''

