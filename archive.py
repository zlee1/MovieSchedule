import pandas as pd
import sqlite3
from duckdb import sql
import datetime
import traceback
import platform

def initialize_db(db_name):
    """Connect to sqlite3 database

    Keyword arguments:
    db_name - name of database

    Returns:
    [database connection, connection cursor]
    """
    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()

def insert_archive(history, cursor):
    for index, row in history.iterrows():
        query = f"""
            INSERT INTO archive(movie_id, theater_id, start_date, end_date)
            VALUES(
                \'{row['movie_id']}\'
                ,\'{row['theater_id']}\'
                ,\'{row['start_date']}\'
                ,\'{row['end_date']}\'
            );
        """

        cursor.execute(query)

def delete_history(history, cursor):
    for index, row in history.iterrows():
        query = f"""
            DELETE FROM showtimes 
            WHERE 1=1
                AND movie_id = \'{row['movie_id']}\'
                AND theater_id = \'{row['theater_id']}\';
        """

        cursor.execute(query)

if __name__ == '__main__':
    try:
        conn, cursor = initialize_db(('\\' if platform.system() == 'Windows' else '/').join(['sqlite3', 'moviedb']))

        history = pd.read_sql("""
            SELECT movie_id, theater_id, MIN(date) AS start_date, MAX(date) AS end_date FROM showtimes s
                WHERE NOT EXISTS(
                    SELECT 1 FROM showtimes s2
                        WHERE 1=1
                            AND s2.movie_id = s.movie_id
                            AND s2.theater_id = s.theater_id
                            AND CAST(strftime(\'%s\', s2.date) AS integer) > CAST(strftime(\'%s\', DATE(\'now\', \'-1 month\')) AS integer)
                )
                GROUP BY movie_id, theater_id
                ORDER BY movie_id, theater_id""", conn)
        
        print(f"""Archiving {sql("SELECT COUNT(*) AS ct FROM history").df()["ct"].iloc[0]} records from before {pd.read_sql("SELECT DATE('now', '-1 month') AS date", conn)["date"].iloc[0]}.""")

        insert_archive(history, cursor)

        delete_history(history, cursor)

        conn.commit()
            
    except Exception:
        print(traceback.format_exc())
    finally:
        conn.close()