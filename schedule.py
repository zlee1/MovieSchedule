import pandas as pd
import sqlite3
from duckdb import sql
import datetime
import traceback
import platform

import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage

def initialize_db(db_name):
    """Connect to sqlite3 database

    Keyword arguments:
    db_name - name of database

    Returns:
    [database connection, connection cursor]
    """
    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()

def send_email(content, subscriber, to, dates=None):
    """Send generated schedule to subscriber

    Keyword arguments:
    content - generated movie schedule
    subscriber - name of the subscriber
    to - email address of subscriber
    dates - [start date of schedule, end date of schedule]

    Returns:
    None
    """

    # default date range is 1 week starting on day of program run
    if(dates is None):
        dates = []
        dates.append((datetime.datetime.today()).strftime('%m/%d/%y'))
        dates.append((datetime.datetime.today() + datetime.timedelta(days=7)).strftime('%m/%d/%y'))

    # read email credentials
    with open(('\\' if platform.system() == 'Windows' else '/').join(['data', 'email_credentials.txt']), 'r') as f:
        host = f.readline().replace('\n', '')
        email = f.readline().replace('\n', '')
        password = f.readline().replace('\n', '')
        
    msg = EmailMessage()

    msg['From'] = email
    msg['To'] = to
    msg['Subject'] = f'Movie Theater Schedule: {dates[0]} - {dates[1]}' 

    header = f'Hi {subscriber}, here is your weekly movie theater rundown:'

    msg.set_content(header + '\n\n' + content)

    # initialize smtp connection
    server = smtplib.SMTP(host, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(email, password)

    server.send_message(msg)

    server.quit()

    print('schedule sent to', subscriber)

def showtime_prettify(showtime_df, movie_df, theater_df, include_schedule = True, include_titles = False, time_count = False):
    """Create formatted schedule.
    
    Keyword arguments:
    showtime_df - dataframe containing showtimes 
    movie_df - dataframe containing movies
    theater_df - dataframe containing theaters
    include_schedule - bool deciding whether to include daily breakdown at each theater
    include_titles - bool deciding whether to include list of all showing titles for each theater
    time_count - bool deciding whether to include raw showtimes or count of showtimes

    Returns:
    string - movie schedule

    Example output:

    AMC Danbury 16

        A24 x IMAX Present: The Green Knight
        AMC Screen Unseen: December 9
        AXCN: BABYMETAL Legend-43 The Movie
        André Rieu's Christmas Concert
        Dr. Seuss' How the Grinch Stole Christmas
        Dr. Seuss' The Grinch
        Elf
        Love Actually
        National Lampoon's Christmas Vacation
        The Metropolitan Opera: The Magic Flute
        The Polar Express
        UFC 310: Pantoja vs. Asakura

        2024-12-06
                Dr. Seuss' The Grinch @ 15:05
                Elf @ 14:00
        2024-12-07
                Dr. Seuss' How the Grinch Stole Christmas @ 12:30
                Elf @ 11:25
                The Metropolitan Opera: The Magic Flute @ 13:00
                UFC 310: Pantoja vs. Asakura @ 22:00
        2024-12-08
                Love Actually @ 17:00
                The Polar Express @ 15:15, 15:25
        2024-12-09
                AMC Screen Unseen: December 9 @ 19:00
                Dr. Seuss' The Grinch @ 14:00
                National Lampoon's Christmas Vacation @ 16:40
        2024-12-10
                Dr. Seuss' How the Grinch Stole Christmas @ 14:00
                Love Actually @ 17:00
        2024-12-11
                A24 x IMAX Present: The Green Knight @ 19:00
                AXCN: BABYMETAL Legend-43 The Movie @ 19:30
                André Rieu's Christmas Concert @ 19:00
                Elf @ 13:35
    """

    # join movie_df, showtime_df, and theater_df
    full_df = sql('SELECT DISTINCT m.name AS movie, m.release_year AS release_year, t.name AS theater, s.date AS date, s.time AS time, s.url AS showtime_url, s.movie_id AS movie_id, s.theater_id AS theater_id, s.id AS showtime_id FROM showtime_df s INNER JOIN movie_df m ON s.movie_id = m.id INNER JOIN theater_df t ON s.theater_id = t.id ORDER BY t.name, s.date, m.name, s.time').df()


    showtime_str = ''
    # loop through each theater
    for t_index, t_row in sql('SELECT DISTINCT theater, theater_id FROM full_df').df().iterrows():
        
        showtime_str += t_row['theater'] + '\n'

        theater_id = t_row['theater_id']

        if(include_titles):
            showtime_str += '\n'

            for index, row in sql(f'SELECT DISTINCT movie FROM full_df WHERE theater_id = \'{theater_id}\' ORDER BY movie').df().iterrows():
                showtime_str += f'\t{row["movie"]}' + '\n' # add year?
            
            showtime_str += '\n'
        if(include_schedule):
            for date in sql(f'SELECT DISTINCT date FROM full_df WHERE theater_id = \'{theater_id}\' ORDER BY date').df()['date']:
                showtime_str += f'\t{date}' + '\n'

                for index, row in sql(f'SELECT movie, date, GROUP_CONCAT(SUBSTR(time, 0, 6), \', \') AS times FROM full_df WHERE theater_id = \'{theater_id}\' AND date = \'{date}\' GROUP BY date, movie ORDER BY movie').df().iterrows():
                    if(time_count):
                        showtime_str += f'\t\t{row["movie"]} ({len(row["times"].split(","))})' + '\n'
                    else:
                        showtime_str += f'\t\t{row["movie"]} @ {row["times"]}' + '\n'
            showtime_str += '\n'
    return showtime_str

def schedule_simple(showtime_df, movie_df, theater_df, new_this_week, limited_showings):
    schedule = ''
    for theater_index, theater_row in theater_df.iterrows():
        schedule += theater_row['name'] + '\n'

        movies = sql(f"""
                     SELECT DISTINCT 
                        s.movie_id
                        ,m.name
                        ,CASE WHEN n.movie_id IS NOT NULL THEN 1 ELSE 0 END AS new
                        ,CASE WHEN l.movie_id IS NOT NULL THEN 1 ELSE 0 END AS limited
                        ,(SELECT COUNT(*) FROM showtime_df s2 GROUP BY s2.movie_id, s2.theater_id HAVING s2.movie_id = s.movie_id AND s2.theater_id = s.theater_id) AS num_showings
                     FROM showtime_df s 
                     INNER JOIN movie_df m ON s.movie_id = m.id 
                     LEFT JOIN new_this_week n ON m.id = n.movie_id AND n.theater_id = s.theater_id
                     LEFT JOIN limited_showings l ON l.movie_id = m.id AND l.theater_id = s.theater_id
                     WHERE s.theater_id = \'{theater_row["id"]}\' 
                     ORDER BY m.name""").df()

        for index, row in movies.iterrows():
            schedule += f"""{'+' if row['new'] else ' '}{'*' if row['limited'] else ' '}{row['name']} [x{row["num_showings"]}]\n"""
        
        schedule += '\n'
    return schedule

if __name__ == '__main__':
    try:
        # connect to database
        conn, cursor = initialize_db(('\\' if platform.system() == 'Windows' else '/').join(['sqlite3', 'moviedb']))

        # initialize dataframes
        subscribers = pd.read_sql('SELECT * FROM subscribers', conn)
        subscriptions = pd.read_sql('SELECT * FROM subscriptions', conn)
        all_theaters = pd.read_sql('SELECT * FROM theaters', conn)
        all_movies = pd.read_sql('SELECT * FROM movies', conn)
        # only include showtimes that occur within next week
        all_showtimes = pd.read_sql('SELECT * FROM showtimes WHERE CAST(strftime(\'%s\', date) AS integer) > CAST(strftime(\'%s\', DATE()) AS integer)', conn)
        # showtimes for movies that have not been shown more than 2 days prior to this week (2-day grace period accounts for early access screenings and thursday previews). 
        # currently does not handle rereleases, but old data is archived monthly so this is not likely to become a problem
        all_new_this_week = pd.read_sql("""
                                        SELECT * FROM showtimes s 
                                            WHERE NOT EXISTS 
                                                (SELECT 1 FROM showtimes s2 
                                                    WHERE 1=1
                                                        AND s2.movie_id = s.movie_id 
                                                        AND s2.theater_id = s.theater_id
                                                        AND CAST(strftime(\'%s\', s2.date) AS integer) <= CAST(strftime(\'%s\', DATE(\'now\', \'-2 days\')) AS integer))""", conn)


        # generate schedule and send email for each subscriber
        for index, row in subscribers.iterrows():
            # schedule_string = ''

            subscriber = row['id']

            # ids of theaters that the subscriber subscribes to
            theater_ids = list(sql(f'SELECT theater_id FROM subscriptions WHERE subscriber_id = {subscriber}').df()['theater_id'])

            # # data only includes theaters that the subscriber subscribes to
            theaters = sql(f'SELECT * FROM all_theaters WHERE id IN {theater_ids}').df()
            showtimes = sql(f'SELECT * FROM all_showtimes WHERE theater_id IN {theater_ids}').df()
            new_this_week = sql(f'SELECT * FROM all_new_this_week WHERE theater_id IN {theater_ids}').df()
            # only movies with 3 or less screenings at a particular theater in the next week. if something is showing 5 times at one theater, but 2 at another, it will be included here only for the theater with 2 screenings
            limited_showings = sql('SELECT movie_id, theater_id, COUNT(*) AS count FROM showtimes GROUP BY movie_id, theater_id HAVING COUNT(*) <= 3 ORDER BY theater_id, movie_id').df()
            
            # # plain text schedule for movies that are new in theaters
            # schedule_string += 'NEW THIS WEEK'
            # schedule_string += '\n\n'
            # schedule_string += showtime_prettify(sql('SELECT * FROM new_this_week').df(), all_movies, all_theaters, include_schedule=False, include_titles=True)

            # # plain text schedule for movies that have 3 or less screenings at a particular theater in the next week
            # schedule_string += '\n\n'
            # schedule_string += 'LIMITED SHOWTIMES'
            # schedule_string += '\n\n'
            # schedule_string += showtime_prettify(sql('SELECT * FROM showtimes WHERE CONCAT(movie_id, theater_id) IN (SELECT CONCAT(movie_id, theater_id) FROM limited_showings)').df(), all_movies, theaters, include_titles=True, time_count=False)

            # # plain text schedule for all screenings
            # schedule_string += '\n\n'
            # schedule_string += 'FULL SCHEDULE'
            # schedule_string += '\n\n'
            # schedule_string += showtime_prettify(showtimes, all_movies, theaters, include_titles=False, time_count=True)
            
            # email the plain text schedule to the subscriber
            schedule = schedule_simple(showtimes, all_movies, theaters, new_this_week, limited_showings)
            send_email(schedule, row['name'], row['email'])
            
    except Exception:
        print(traceback.format_exc())
    finally:
        conn.close()