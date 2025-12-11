def subs_over_time_count(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            EXTRACT(HOUR FROM datetime_added) AS hour,
            COUNT(*) AS submission_count
        FROM analytical_responses
        GROUP BY hour
        ORDER BY hour
        """
    )
    
    results = cursor.fetchall()
    cursor.close()
    
    # Create a dictionary with hours that have submissions
    hour_counts = {int(row[0]): row[1] for row in results}
    
    # Generate all 24 hours
    data_list = []
    for hour in range(8, 19):  # From 8 AM to 6 PM
        time_str = f"{hour:02d}:00"
        count = hour_counts.get(hour, 0)  # Default to 0 if no submissions
        data_list.append({"x": time_str, "y": count})
    
    return {"datasets": [{"id": "Submissions", "data": data_list}]}

# {
#   "datasets": [
#     {
#       "id": "Submissions",
#       "data": [
#         { "x": "12:00", "y": 82 },
#         { "x": "13:00", "y": 88 },
#         { "x": "14:00", "y": 101 },
#         { "x": "15:00", "y": 97 },
#         { "x": "16:00", "y": 121 },
#         { "x": "17:00", "y": 83 },
#         { "x": "18:00", "y": 59 }
#       ]
#     }
#   ]
# }