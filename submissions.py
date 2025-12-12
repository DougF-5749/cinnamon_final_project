def submission_count(connection, db_state: dict) -> dict:
    last_max_id = db_state['last_max_id']

    cursor = connection.cursor() 

    cursor.execute(
        """
        SELECT
            COUNT(*),
            MAX(id)
        FROM analytical_responses
        WHERE id > %s
        """, (last_max_id,)
    )    
    
    row_count, max_id = cursor.fetchone()

    cursor.close()    
    
    db_state['total_submissions'] += row_count

    if max_id is not None:
        db_state['last_max_id'] = max_id
    # else:  
    #     db_state['last_max_id'] = last_max_id

    return {"count": db_state['total_submissions']}

