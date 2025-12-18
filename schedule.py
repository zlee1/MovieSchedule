import pandas as pd
import sqlite3
from duckdb import sql
import datetime
import traceback
import platform
import logging
import os
import requests
import sys

import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger('schedule')


def initialize_db(db_name):
    """Connect to sqlite3 database

    Keyword arguments:
    db_name - name of database

    Returns:
    [database connection, connection cursor]
    """
    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()

def send_email(content, subscriber, to, subscriber_id, html=False, dates=None):
    """Send generated schedule to subscriber

    Keyword arguments:
    content - generated movie schedule
    subscriber - name of the subscriber
    to - email address of subscriber
    html - whether the email should be sent as html
    dates - [start date of schedule, end date of schedule]

    Returns:
    None
    """
    
    # default date range is 1 week starting on day of program run
    if(dates is None):
        dates = []
        dates.append((datetime.datetime.today()).strftime('%m/%d/%y'))
        dates.append((datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%m/%d/%y'))
    
    # read email credentials
    with open(os.path.join('data', 'email_credentials.txt'), 'r') as f:
        host = f.readline().replace('\n', '')
        email = f.readline().replace('\n', '')
        password = f.readline().replace('\n', '')
    
    msg = MIMEMultipart()

    msg['From'] = email
    msg['To'] = to
    msg['Subject'] = f'Movie Theater Schedule: {dates[0]} - {dates[1]}' 

    if(html):
        msg.attach(MIMEText(content, 'html'))
    else:
        msg.attach(MIMEText(content, 'plain'))

    # initialize smtp connection
    server = smtplib.SMTP(host, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    
    server.login(email, password)
    
    server.sendmail(email, to, msg.as_string())
    
    server.quit()

    logger.info(f'Schedule sent to {subscriber_id}: {subscriber}')

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
            schedule += f"""{'+' if row['new'] else ' '}{'*' if row['limited'] else ' '} {row['name']} [x{row["num_showings"]}]\n"""
        
        schedule += '\n'
    return schedule

def schedule_simple_html(showtime_df, movie_df, theater_df, new_this_week, limited_showings, subscriber, by='both'):
    schedule = """
<html>

<head>
    <style>
        body {
            font-family: 'Consolas', monospace;
        }
        h1 {
            font-size: 22px;
        }
        h2 {
            font-size: 18px;
            margin-bottom: 0;
        }
        p {
            font-size: 14px;
            margin: 0;
            padding-left: 20px;
        }
    </style>
</head>

<body>

    <p style="padding-left: 0;">Hi, %s! Here is your weekly theatrical breakdown:</p>

    <br>

    <p style="padding-left: 0;">Showings new this week are <b>bolded</b></p>
    <p style="padding-left: 0;">Showings with 3 or fewer screenings are <span style="color:#AA0000">red</span></p>

    <br>
""" % subscriber

    if(by in ['both', 'theater']):
        schedule += '<h1>Breakdown by Theater</h1>'

        for theater_index, theater_row in theater_df.iterrows():
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
                        
            if(len(movies) == 0):
                continue
            else:
                schedule += f"\t<h2>{theater_row['name']}</h2>\n"

                for index, row in movies.iterrows():
                    schedule += f"""\t<p{' style="color:#AA0000"' if row['limited'] else ''}>{'<b>' if row['new'] else ''}{row['name']} [x{row["num_showings"]}]{'</b>' if row['new'] else ''}</p>\n"""
            
    if(by == 'both'):
        schedule += '<br><br><br><h1>Breakdown by Film</h1>'

    if(by in ['both', 'movie']):
        for movie_index, movie_row in movie_df.sort_values(by=['name'], inplace=False).iterrows():
            schedule += f"\t<h2>{movie_row['name']}</h2>\n"

            theaters = sql(f"""
                        SELECT DISTINCT 
                            t.id
                            ,t.name
                            ,CASE WHEN n.theater_id IS NOT NULL THEN 1 ELSE 0 END AS new
                            ,CASE WHEN l.theater_id IS NOT NULL THEN 1 ELSE 0 END AS limited
                            ,(SELECT COUNT(*) FROM showtime_df s2 GROUP BY s2.movie_id, s2.theater_id HAVING s2.movie_id = s.movie_id AND s2.theater_id = s.theater_id) AS num_showings
                        FROM showtime_df s
                        INNER JOIN theater_df t ON t.id = s.theater_id
                        LEFT JOIN new_this_week n ON n.movie_id = s.movie_id AND n.theater_id = t.id
                        LEFT JOIN limited_showings l ON l.movie_id = s.movie_id AND l.theater_id = t.id
                        WHERE s.movie_id = \'{movie_row["id"]}\'
                        --GROUP BY t.id, t.name, s.movie_id
                        ORDER BY t.name""").df()
            
            for index, row in theaters.iterrows():
                schedule += f"""\t<p{' style="color:#AA0000"' if row['limited'] else ''}>{'<b>' if row['new'] else ''}{row['name']} [x{row["num_showings"]}]{'</b>' if row['new'] else ''}</p>\n"""
        
    
    schedule += '</body>\n</html>'
    return schedule 

def schedule_styled_html(showtime_df, movie_df, theater_df, new_this_week, limited_showings, subscriber):
    with open('email_base_template.html', 'r') as f:
        base_template = f.read()
    
    with open('email_film_template.html', 'r') as f:
        film_template = f.read()

    films = []
    for movie_index, movie_row in movie_df.sort_values(by=['name'], inplace=False).iterrows():
        theaters = sql(f"""
                    SELECT DISTINCT 
                        t.id
                        ,t.name
                        ,CASE WHEN n.theater_id IS NOT NULL THEN 1 ELSE 0 END AS new
                        ,CASE WHEN l.theater_id IS NOT NULL THEN 1 ELSE 0 END AS limited
                        ,(SELECT COUNT(*) FROM showtime_df s2 GROUP BY s2.movie_id, s2.theater_id HAVING s2.movie_id = s.movie_id AND s2.theater_id = s.theater_id) AS num_showings
                    FROM showtime_df s
                    INNER JOIN theater_df t ON t.id = s.theater_id
                    LEFT JOIN new_this_week n ON n.movie_id = s.movie_id AND n.theater_id = t.id
                    LEFT JOIN limited_showings l ON l.movie_id = s.movie_id AND l.theater_id = t.id
                    WHERE s.movie_id = \'{movie_row["id"]}\'
                    --GROUP BY t.id, t.name, s.movie_id
                    ORDER BY t.name""").df()
        
        cur_template = '%s' % film_template
        film_header = movie_row['name']
        if(movie_row['release_year'] is not None and not pd.isna(movie_row['release_year'])):
            film_header += f' ({int(movie_row["release_year"])})'

        film_details = ''
        
        if(movie_row['runtime'] is not None and not pd.isna(movie_row['runtime'])):
            film_details += str(int(movie_row['runtime'])) + ' min'
        if(movie_row['rating'] is not None and not pd.isna(movie_row['rating'])):
            film_details += f'{", " if film_details != "" else ""}{movie_row["rating"]}'

        cur_template = cur_template.replace('{header}', film_header)
        cur_template = cur_template.replace('{details}', film_details)
        cur_template = cur_template.replace('{film_url}', movie_row['url'])
        cur_template = cur_template.replace('{image_url}', movie_row['image_url'])

        if(movie_row['rt_critic'] is not None and not pd.isna(movie_row['rt_critic']) and movie_row['rt_critic'] != 'NULL' and movie_row['rt_audience'] is not None and not pd.isna(movie_row['rt_audience']) and movie_row['rt_audience'] != 'NULL'):
            cur_template = cur_template.replace('{rt_critic}', movie_row['rt_critic'])
            cur_template = cur_template.replace('{rt_audience}', movie_row['rt_audience'])
        else:
            cur_template = cur_template.replace('{rt_critic}', '--')
            cur_template = cur_template.replace('{rt_audience}', '--')

        if(movie_row['genres'] is not None and not pd.isna(movie_row['genres']) and movie_row['genres'] != ''):
            cur_template = cur_template.replace('{genres}', movie_row['genres'])
        else:
            cur_template = cur_template.replace('{genres}', 'N/A')

        if(movie_row['synopsis'] is not None and not pd.isna(movie_row['synopsis']) and movie_row['synopsis'] != ''):
            cur_template = cur_template.replace('{synopsis}', movie_row['synopsis'])
        else:
            cur_template = cur_template.replace('{synopsis}', 'N/A')
        

        theater_html = []
        for index, row in theaters.iterrows():
            theater_html.append(f"""\t<span style="margin-top: 0.5em;{'color:red;' if row['limited'] else ''}">{'<i>' if row['new'] else ''}{row['name']} <sup>x{row["num_showings"]}</sup>{'</i>' if row['new'] else ''}</p>\n""")
        
        cur_template = cur_template.replace('{theaters}', '<br>'.join(theater_html))

        films.append(cur_template)

    return base_template.replace('{films}', '\n'.join(films)).replace('{user}', subscriber)

def run(test=False, specific_subscribers=None):
    try:

        global logger
        start_time = datetime.datetime.now()

        with open(os.path.join('data', 'file_locations.txt'), 'r') as f:
            file_locations = f.read().splitlines()

        for i in file_locations:
            if(i.startswith('app_db=')):
                app_db = i.split('app_db=')[1]
        
        # setting up logging
        log_location = os.path.join('logs', f'movie_schedule_{datetime.datetime.now().strftime("%d%m%Y")}.log')
        if(not os.path.isfile(log_location)):
            open(log_location, 'w+')
        else:
            with open(log_location, 'a') as f:
                f.write('\n\n\n')

        logging.basicConfig(filename=log_location, level=logging.INFO)
        logger.info(f'Starting {start_time.strftime("%m/%d/%Y %H:%M:%S")}')

        logger.info('Initializing database connections')
        # connect to database
        conn, cursor = initialize_db(os.path.join('sqlite3', 'moviedb')) 
        # app_conn, app_cursor = initialize_db(app_db)

        logger.info('Initializing dataframes')
        # initialize dataframes
        api_subscribers = pd.DataFrame(requests.get(os.environ['WEBAPP_BASEURL'] + 'api/users/', headers={'Authorization': f'Token {os.environ["API_KEY"]}'}).json())
        api_subscriptions = pd.DataFrame(requests.get(os.environ['WEBAPP_BASEURL'] + 'api/subscriptions/', headers={'Authorization': f'Token {os.environ["API_KEY"]}'}).json())
        
        subscribers = sql('SELECT u.id, username, first_name, email FROM api_subscribers u INNER JOIN (SELECT DISTINCT user_id FROM api_subscriptions) s ON s.user_id = u.id WHERE is_active=1').df()
        subscriptions = sql('SELECT user_id, theater_id FROM api_subscriptions s INNER JOIN api_subscribers u ON u.id = s.user_id WHERE u.is_active = 1').df()
        # zip_codes = pd.read_sql('SELECT * FROM zip_codes', conn)
        all_theaters = pd.read_sql('SELECT * FROM theaters', conn)
        all_movies = pd.read_sql('SELECT * FROM movies', conn)
        # only include showtimes that occur within next week
        all_showtimes = pd.read_sql('SELECT * FROM showtimes WHERE CAST(strftime(\'%s\', date) AS integer) > CAST(strftime(\'%s\', DATE(\'now\', \'localtime\')) AS integer)', conn)
        # showtimes for movies that have not been shown more than 2 days prior to this week (2-day grace period accounts for early access screenings and thursday previews). 
        # currently does not handle rereleases, but old data is archived monthly so this is not likely to become a problem
        all_new_this_week = pd.read_sql("""
                                        SELECT * FROM showtimes s 
                                            WHERE 1=1
                                                AND NOT EXISTS 
                                                    (SELECT 1 FROM showtimes s2 
                                                        WHERE 1=1
                                                            AND s2.movie_id = s.movie_id 
                                                            AND s2.theater_id = s.theater_id
                                                            AND CAST(strftime(\'%s\', s2.date) AS integer) <= CAST(strftime(\'%s\', DATE(\'now\', \'-2 days\', \'localtime\')) AS integer))
                                                AND EXISTS
                                                    (SELECT 1 FROM showtimes s2
                                                        WHERE 1=1
                                                            AND s2.theater_id = s.theater_id
                                                            AND CAST(strftime(\'%s\', s2.date) AS integer) <= CAST(strftime(\'%s\', DATE(\'now\', \'-6 days\', \'localtime\')) AS integer))
                                        """, conn)


        logger.info('Starting schedule process')

        # read email credentials
        with open(os.path.join('data', 'email_credentials.txt'), 'r') as f:
            host = f.readline().replace('\n', '')
            email = f.readline().replace('\n', '')
            password = f.readline().replace('\n', '')
            test_email = f.readline().replace('\n', '') # when run in test mode, send all emails to test email instead of real users

        if(test):
            logger.warning(f'Running in test mode - all schedule emails will go to {test_email}')

        # generate schedule and send email for each subscriber
        for index, row in subscribers.iterrows():

            subscriber_id = row['id']
            
            if(specific_subscribers is not None and str(subscriber_id) not in specific_subscribers):
                continue

            first_name = row['first_name']
            subscriber_name = first_name if first_name is not None and first_name != '' else row['username']
            subscriber_email = row['email']

            logger.info(f'Schedule for {subscriber_name}')

            logger.info('Gathering subscription-specific data')
            # ids of theaters that the subscriber subscribes to
            theater_ids = list(sql(f'SELECT DISTINCT theater_id FROM subscriptions WHERE user_id = {subscriber_id}').df()['theater_id'])

            # # data only includes theaters that the subscriber subscribes to
            theaters = sql(f'SELECT * FROM all_theaters WHERE id IN {theater_ids} ORDER BY name').df()
            showtimes = sql(f'SELECT * FROM all_showtimes WHERE CAST(theater_id as varchar(15)) IN {theater_ids}').df()
            movies = sql(f'SELECT * FROM all_movies WHERE id IN (SELECT movie_id FROM showtimes)').df()
            new_this_week = sql(f'SELECT * FROM all_new_this_week WHERE CAST(theater_id AS varchar(15)) IN {theater_ids}').df()
            # only movies with 3 or less screenings at a particular theater in the next week. if something is showing 5 times at one theater, but 2 at another, it will be included here only for the theater with 2 screenings
            limited_showings = sql('SELECT movie_id, theater_id, COUNT(*) AS count FROM showtimes GROUP BY movie_id, theater_id HAVING COUNT(*) <= 3 ORDER BY theater_id, movie_id').df()

            logger.info('Generating schedule')
            # generate and email html schedule
            # schedule = schedule_simple_html(showtimes, movies, theaters, new_this_week, limited_showings, subscriber=subscriber_name)
            schedule = schedule_styled_html(showtimes, movies, theaters, new_this_week, limited_showings, subscriber=subscriber_name)
            logger.info('Emailing schedule')
            send_email(schedule, subscriber_name, subscriber_email if not test else test_email, subscriber_id, html=True) # if test mode active send all emails to test emails

    except Exception:
        logger.error(traceback.format_exc())
    finally:
        conn.close()
        # app_conn.close()

        end_time = datetime.datetime.now()
        logger.info(f'Finished {end_time.strftime("%m/%d/%Y %H:%M:%S")}, total runtime: {(end_time-start_time).total_seconds()} seconds')

if __name__ == "__main__":
	run(test=False, specific_subscribers=sys.argv[1:])
