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
    for hour in range(24):
        time_str = f"{hour:02d}:00"
        count = hour_counts.get(hour, 0)  # Default to 0 if no submissions
        data_list.append({"x": time_str, "y": count})
    
    return {"datasets": [{"id": "Submissions", "data": data_list}]}