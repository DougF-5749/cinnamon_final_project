def submission_count(connection, db_state: dict) -> dict:
    last_max_id = db_state['last_max_id']

    cursor = connection.cursor() 

    cursor.execute(
        """
        SELECT
            COUNT(*),
            MAX(id)
        FROM responses
        WHERE id > %s
        """, last_max_id
    )    
    
    row_count, max_id = cursor.fetchone()

    cursor.close()    
    
    db_state['total_submissions'] += row_count
    db_state['last_max_id'] = max_id

    return {"count": db_state['total_submissions']}


