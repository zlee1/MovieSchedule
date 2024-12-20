import pandas as pd
import sqlite3
from duckdb import sql
import datetime
import traceback
import platform
import os

import logging

logger = logging.getLogger('archive')

def initialize_db(db_name):
    """Connect to sqlite3 database

    Keyword arguments:
    db_name - name of database

    Returns:
    [database connection, connection cursor]
    """
    logger.info('Initializing database connection')

    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()

def insert_archive(history, cursor):
    logger.info('Inserting data into archive')

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
    logger.info('Deleting original data')

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

        start_time = datetime.datetime.now()

        log_location = ('\\' if platform.system() == 'Windows' else '/').join(['logs', f'movie_schedule_{datetime.datetime.now().strftime("%d%m%Y")}.log'])
        if(not os.path.isfile(log_location)):
            open(log_location, 'w+')
        else:
            with open(log_location, 'a') as f:
                f.write('\n\n\n')

        logging.basicConfig(filename=log_location, level=logging.INFO)
        logger.info(f'Starting {start_time.strftime("%m/%d/%Y %H:%M:%S")}')


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
        
        if(len(history) > 0):
            logger.info(f"""Archiving {sql("SELECT COUNT(*) AS ct FROM history").df()["ct"].iloc[0]} records from before {pd.read_sql("SELECT DATE('now', '-1 month') AS date", conn)["date"].iloc[0]}""")
            
            insert_archive(history, cursor)

            delete_history(history, cursor)

            logger.info('Committing changes to database')
            conn.commit()
        else:
            logger.info('No old data to archive')

        for i in os.listdir('logs'):
            if('movie_schedule' in i and datetime.datetime.strptime(i.split('_')[-1].split('.')[0], '%d%m%Y').date() < datetime.datetime.now().date() - datetime.timedelta(days=6)):
                logger.info(f'Deleting old log {i}')
                os.remove(('\\' if platform.system() == 'Windows' else '/').join(['logs', i]))

    except Exception:
        logger.error(traceback.format_exc())
    finally:

        logger.info('Closing database connection')
        conn.close()

        end_time = datetime.datetime.now()
    
        logger.info(f'Finished {end_time.strftime("%m/%d/%Y %H:%M:%S")}, total runtime: {(end_time-start_time).total_seconds()} seconds')

