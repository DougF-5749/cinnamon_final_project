def get_adb_conn(conn_pool):
    conn = conn_pool.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn_pool.putconn(conn)