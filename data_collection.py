from time import sleep
import random
from datetime import datetime
from datetime import timedelta
import re
import traceback
import pandas as pd
import sqlite3
import platform
import logging
import os

import warnings
warnings.filterwarnings("ignore")

from bs4 import BeautifulSoup

import urllib.request
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
# from subprocess import CREATE_NO_WINDOW

import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage


logger = logging.getLogger('data_collection')

log_location = None
driver_location = None


def browser_init():
    """Create Selenium browser instance.
    
    Returns:
    selenium browser instance
    """
    logger.info('Creating new browser instance')

    options = Options()

    # options.add_argument("--headless=new") # run browser without opening window
    options.add_argument("--log-level=3") # log only errors
    options.add_argument('--blink-settings=imagesEnabled=false') # prevent image loading (?)

    service = Service(executable_path=driver_location)
        
    # service.creationflags = CREATE_NO_WINDOW # fully suppress selenium logging

    driver = webdriver.Firefox(options=options, service=service)
    driver.set_page_load_timeout(300)
    
    logger.info('New browser created')

    return driver

def initialize_db(db_name):
    """Connect to sqlite3 database

    Keyword arguments:
    db_name - name of database

    Returns:
    [database connection, connection cursor]
    """
    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()

def get_zip_codes(conn):
    """Get list of zip codes to be used for locating theaters. 

    Keyword Arguments:
    from_file - whether to read zip codes from file (1) or take user input (0)
    filename - relative path to read zip codes from when from_file == 1

    Returns:
    list - [str zip code, str zip code, str zip code]
    """

    # if(filename is None):
    #     filename = ('\\' if platform.system() == 'Windows' else '/').join(['data', 'data.txt'])

    # if(not from_file):
    #     zip_codes = []
    #     zip_code = None
    #     while zip_code != '':
    #         zip_code = str(input('Enter a zip code (empty for done): ')).strip()
    #         if(zip_code != ''):
    #             zip_codes.append(zip_code)
    #     logger.info(f'zip codes: {zip_codes}')
    #     return zip_codes
    # else:
    #     with open(filename, 'r') as f:
    #         for i in f.readlines():
    #             if(i[:3] == 'zip'):
    #                 zip_codes = re.sub('zip=', '', i).replace('\n', '').split(',')
    #                 logger.info(f'zip codes: {zip_codes}')
    #                 return zip_codes

    # logger.info(list(pd.read_sql('SELECT DISTINCT zip_code FROM subscriptions', conn)['zip_code']))

    return list(pd.read_sql('SELECT DISTINCT zip_code FROM subscriptions WHERE active=1;', conn)['zip_code'])

def get_soup(theater, url, date, browser):
    formatted_date = date.strftime('%Y-%m-%d')

    full_url = f'{url}?cmp=theater-module&format=all&date={formatted_date}'
    
    logger.info(f'current theater: {theater} | current date: {date} | address: {full_url}')
    
    for i in range(10):

        browser.get(full_url)

        sleep(random.randint(5, 10))

        soup = BeautifulSoup(browser.page_source, 'html.parser')

        container = soup.find('h1', 'offline__header')
        if(container is None):
            break;
        else:
            logger.warning('offline')
            sleep(60)
    return soup

def get_text(soup):
    return soup.text.replace('\n', '').replace('\t', '').replace('\'', '\'\'').strip()

def select_all_from_table(tablename, conn):
    return pd.read_sql_query(f'SELECT * FROM {tablename}', conn)

def insert_zip_code(zip_code, theater_id, cursor):
    
    query = f"""
        INSERT OR IGNORE INTO zip_codes(zip_code, theater_id)
        VALUES(
            \'{zip_code}\'
            ,\'{theater_id}\'
        );
        """
    
    cursor.execute(query)

def get_theaters(zip_codes, conn, cursor):
    """Get list of all theaters that appear in search for each provided zip code.

    Keyword arguments:
    zip_codes - list of zip codes as string

    Returns:
    list - [ { id : theater id , name : theater name , url : theater url } ]
    """

    theater_list = []
    for zip_code in zip_codes:
        url = f'https://www.fandango.com/{zip_code}_movietimes'

        zip_search = urllib.request.urlopen(url)
        zip_search_page = BeautifulSoup(zip_search.read().decode('utf8'), 'html.parser')

        sleep(15)

        zip_search.close()

        theaters = zip_search_page.find(id='nearby-theaters-select-list').find_all('option')

        for theater in theaters[1:]:
            theater_dict = {}
            theater_dict['id'] = theater["value"].replace('/', '').replace('theater-page', '')[-5:]
            theater_dict['name'] = theater.text.strip()
            theater_dict['url'] = f'https://www.fandango.com{theater["value"]}'
            theater_dict['address'] = None
            if(theater_dict not in theater_list):
                theater_list.append(theater_dict)

            logger.info(f'Adding {theater_dict["name"]} for zip code {zip_code}')
            insert_zip_code(zip_code, theater_dict['id'], cursor)
    
    logger.info('Inserting theater data')
    insert_theaters(theater_list, conn, cursor)

def insert_theaters(theaters, conn, cursor):
    for theater in theaters:
        query = f"""
        INSERT INTO theaters(id, name, url, address)
        VALUES(
            \'{theater.get('id')}\'
            ,\'{theater.get('name').replace('\'', '\'\'')}\'
            ,\'{theater.get('url') if theater.get('url') != None else ''}\'
            ,\'\'
        )
        ON CONFLICT(id) DO UPDATE SET
            name = COALESCE(excluded.name, name)
            ,address = COALESCE(excluded.address, address)
            WHERE url = excluded.url
        ;
        """

        cursor.execute(query)

    conn.commit()

def get_movies_from_theater(soup):
    movies = []
    
    container = soup.find('ul', 'thtr-mv-list')

    if(container is None):
        logger.warning('no movies found')
        return movies

    for movie in container.find_all('li'):
        if(not movie.parent.__eq__(container)): # list item must be direct child of container
            continue;

        movie_id = movie['id'].replace('movie-', '')

        image_sect = movie.find('div').find('a')
        try:
            # accounting for 2 types of image link storage
            if('data-fd-lazy-image' in image_sect.attrs):
                movie_image_url = image_sect['data-fd-lazy-image']
            else:
                movie_image_url = image_sect['style'].replace('background-image: url(\"', '').replace('\");', '')
        except:
            logger.warning(f'no image found for movie {movie_id}')
            movie_image_url = None

        detail_sect = movie.find('div', 'thtr-mv-list__detail')

        title_sect = detail_sect.find('h2', 'thtr-mv-list__detail-title')
        movie_name = get_text(title_sect)

        movie_year = None
        try:
            if(re.match(r'\([0-9]{4}\)', get_text(title_sect)[-5:])):
                movie_year = int(get_text(title_sect)[-5:].replace('(', '').replace(')', ''))
        except Exception:
            movie_year = None
            logger.warning(f'year not found for {movie_name}')
        
        if(movie_year is not None):
            movie_name = movie_name[:-7]

        movie_url = 'https://www.fandango.com' + title_sect.find('a')['href']

        movie_info_sect = detail_sect.find('li')
        info_text = get_text(movie_info_sect)
        try:
            movie_rating = info_text.split(', ')[0]
        except Exception as e:
            movie_rating = None
            logger.warning(f'{e}, error with parsing rating')
        
        try:
            if('min' not in info_text.split(', ')[1]):
                movie_runtime = int(info_text.split(', ')[1].replace(' ', '').replace('hr', ''))*60
                logger.warning(f'hr only runtime {movie_runtime}')
            else:
                raw_runtime = info_text.split(', ')[1].replace(' min', '').replace(' ', '').split('hr')

                movie_runtime = int(raw_runtime[0])*60 + int(raw_runtime[1])
        except Exception as e:
            movie_runtime = None
            logger.warning(f'{e}, error with parsing runtime from {info_text}')
        
        movies.append(
            {
                'id': movie_id
                ,'name': movie_name
                ,'url': movie_url
                ,'release_year': movie_year
                ,'runtime': movie_runtime
                ,'rating': movie_rating
                ,'image_url': movie_image_url
            }
        )
        
    return movies

def get_showtimes_from_theater(soup):
    showtimes = []

    container = soup.find('ul', 'thtr-mv-list')

    if(container is None):
        logger.warning('no movies found')
        return showtimes

    for movie in container.find_all('li'):
        if(not movie.parent.__eq__(container)): # list item must be direct child of container
            continue;

        for showtime_sect in movie.find_all('div', 'thtr-mv-list__amenity-group'):
            for showtime_btn in showtime_sect.find_all('li', 'showtimes-btn-list__item'):
                showtime = showtime_btn.find('a')

                if(showtime is None): # showtime took place in the past
                    continue;

                showtime_url = showtime['href']

                movie_id = movie['id'].replace('movie-', '')

                theater_id = showtime_url.split('tid=')[1].split('&')[0].lower()

                showtime_date = showtime_url.split('sdate=')[1].split('%')[0]

                showtime_time = get_text(showtime)

                if('p' in showtime_time):
                    if(showtime_time.split(':')[0] != '12'):
                        showtime_time = f"{int(showtime_time.split(':')[0])+12}:{showtime_time.split(':')[1].replace('p', ':00')}"
                    else:
                        showtime_time = showtime_time.replace('p', ':00')
                else:
                    showtime_time = showtime_time.replace('a', ':00')
                    if(len(showtime_time.split(':')[0]) == 1):
                        showtime_time = '0' + showtime_time

                showtime_id = f'{movie_id}_{theater_id}_{showtime_date}_{showtime_time}'

                showtime_format = None

                showtimes.append(
                    {
                        'id': showtime_id
                        ,'movie_id': movie_id
                        ,'theater_id': theater_id
                        ,'url': showtime_url
                        ,'date': showtime_date
                        ,'time': showtime_time
                        ,'format': showtime_format
                    }
                )

    return showtimes

def get_all_movies_and_showtimes(theaters, dates, browser, conn, cursor, redo=False):
    # skip theaters that have showtime data one week away - these have already gone through the data collection process
    # smaller theaters that do not have screenings one week away but do have screenings within the following week will be rechecked in this scenario, but this is uncommon and shouldn't be an issue
    if(not redo):
        skip_theaters = list(pd.read_sql('SELECT DISTINCT id FROM theaters WHERE date_updated = DATE(\'now\', \'localtime\')', conn)['id'])
    else:
        skip_theaters = []

    for index, row in theaters.iterrows():
        # logger.info(f'Theater switching to {row["name"]}')
        if(row['id'] in skip_theaters):
            logger.info(f'Skipping theater {row["name"]} - data already collected')
            continue;

        new_movies = []
        new_showtimes = []
        for date in dates:
            if(date <= datetime.strptime(row['date_updated'], '%Y-%m-%d').date()+timedelta(days=6)):
                logger.info(f'Skipping date {datetime.strftime(date, "%Y-%m-%d")} for theater {row["name"]} - data already collected.')
                continue
            soup = get_soup(row['name'], row['url'], date, browser)
            
            new_movies = get_movies_from_theater(soup)
            new_showtimes = get_showtimes_from_theater(soup)
        
        logger.info(f'Inserting movies and showtimes for {row["name"]}')
        if(new_movies != []):
            insert_movies(new_movies, conn, cursor)
        if(new_showtimes != []):
            insert_showtimes(new_showtimes, conn, cursor)

        logger.info(f'Updating theater date_updated for {row["name"]}')
        theater_date_update(row['id'], conn, cursor)

        # logger.info('Closing browser')
        # browser.quit()

        # browser = browser_init()

def theater_date_update(theater_id, conn, cursor):
    cursor.execute(f"UPDATE theaters SET date_updated = CURRENT_DATE WHERE id=\'{theater_id}\';")
    conn.commit()

def insert_movies(movies, conn, cursor):
    for movie in movies:
        query = f"""
        INSERT INTO movies(id, name, url, release_year, runtime, rating, image_url)
        VALUES(
            \'{movie.get('id')}\'
            ,\'{movie.get('name')}\'
            ,\'{movie.get('url')}\'
            ,{movie.get('release_year') if movie.get('release_year') != None else 'NULL'}
            ,{movie.get('runtime') if movie.get('runtime') != None else 'NULL'}
            ,\'{movie.get('rating') if movie.get('rating') != None else ''}\'
            ,\'{movie.get('image_url') if movie.get('image_url') != None else ''}\'
        )
        ON CONFLICT(id) DO UPDATE SET
            name = COALESCE(excluded.name, name)
            ,release_year = COALESCE(excluded.release_year, release_year)
            ,runtime = COALESCE(excluded.runtime, runtime)
            ,rating = COALESCE(excluded.rating, rating)
            ,image_url = COALESCE(excluded.image_url, image_url)
            WHERE url = excluded.url
        ;
        """

        cursor.execute(query)
    conn.commit()

def insert_showtimes(showtimes, conn, cursor):
    for showtime in showtimes:
        query = f"""
        INSERT INTO showtimes(id, movie_id, theater_id, url, date, time, format)
        VALUES(
            \'{showtime.get('id')}\'
            ,\'{showtime.get('movie_id')}\'
            ,\'{showtime.get('theater_id')}\'
            ,\'{showtime.get('url')}\'
            ,\'{showtime.get('date')}\'
            ,\'{showtime.get('time')}\'
            ,\'{showtime.get('format') if showtime.get('format') != None else ''}\'
        )
        ON CONFLICT(id) DO UPDATE SET
            movie_id = COALESCE(excluded.movie_id, movie_id)
            ,theater_id = COALESCE(excluded.theater_id, theater_id)
            ,date = COALESCE(excluded.date, date)
            ,time = COALESCE(excluded.time, time)
            ,format = COALESCE(excluded.format, format)
            ,date_inserted = CURRENT_DATE
            WHERE url = excluded.url
        ;
        """

        cursor.execute(query)

    conn.commit()

def run():
    conn = None
    driver = None

    try:
        logger.info('Initializing browser')
        driver = browser_init()

        logger.info('Connecting to database')
        conn, cursor = initialize_db(('\\' if platform.system() == 'Windows' else '/').join(['sqlite3', 'moviedb']))

        zip_codes = get_zip_codes(conn)
        
        logger.info('Collecting theaters')
        theaters = get_theaters(zip_codes, conn, cursor)

        # logger.info('Inserting theater data to db')
        # insert_theaters(theaters, conn, cursor)
        zip_code_str = ','.join([f"\'{i}\'" for i in zip_codes])
        theater_df = pd.read_sql(f'SELECT * FROM theaters t INNER JOIN zip_codes z ON z.theater_id = t.id WHERE z.zip_code IN ({zip_code_str})', conn)

        logger.info('Collecting movies and showtimes')
        get_all_movies_and_showtimes(theater_df, [datetime.now().date() + timedelta(days=i) for i in range(7)], driver, conn, cursor, redo=False)

        # logger.info('Inserting movie data to db')
        # insert_movies(movies, cursor)

        # logger.info('Inserting showtime data to db')
        # insert_showtimes(showtimes, cursor)

        # movie_df = select_all_from_table('movies', conn)

        # showtime_df = select_all_from_table('showtimes', conn)

        # logger.info('Committing db changes')
        # conn.commit()

    except Exception:
        logging.error(traceback.format_exc())
        success = 0
    else:
        success = 1
    finally:
        try: conn.close() 
        except: pass
        try: driver.close()
        except: pass

        logger.info('Closed db connection and webdriver')
        return success


def send_failure_email():
    # read email credentials
    with open(('\\' if platform.system() == 'Windows' else '/').join(['data', 'email_credentials.txt']), 'r') as f:
        host = f.readline().replace('\n', '')
        email = f.readline().replace('\n', '')
        password = f.readline().replace('\n', '')
        error_email = f.readline().replace('\n', '')

    msg = EmailMessage()

    msg['From'] = email
    msg['To'] = error_email
    msg['Subject'] = f'Movie Theater Data Collection Failed at {datetime.now().strftime("%m/%d/%Y %H:%M:%S")}' 


    with open(log_location, 'r') as f:
        log = f.read()

    msg.set_content('Please check logs for more information.')

    #msg.attach(MIMEText(log))

    # initialize smtp connection
    server = smtplib.SMTP(host, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(email, password)

    server.send_message(msg)

    server.quit()

    logger.info('Failure notification sent')



if __name__ == '__main__':
    start_time = datetime.now()

    with open(('\\' if platform.system() == 'Windows' else '/').join(['data', 'file_locations.txt']), 'r') as f:
        file_locations = f.read().splitlines()

    for i in file_locations:
        if(i.startswith('log=')):
            log_location = i.split('log=')[1]
        elif(i.startswith('driver=')):
            driver_location = i.split('driver=')[1]

    log_location = ('\\' if platform.system() == 'Windows' else '/').join(['logs', f'movie_schedule_{datetime.now().strftime("%d%m%Y")}.log'])
    if(not os.path.isfile(log_location)):
        open(log_location, 'w+')
    else:
        with open(log_location, 'a') as f:
            f.write('\n\n\n')

    # if(log_location is None):
    #     warnings.warn('No log provided, creating new log')
    #     with open('movie_schedule.log', 'w+') as f:
    #         f.write('LOG NOT PROVIDED, NEW LOG CREATED')

    if(driver_location is None):
        raise Exception('WebDriver not provided. Please add WebDriver filepath to data/file_locations.txt on a new line in the format of "driver=<filepath>"')

    logging.basicConfig(filename=log_location, level=logging.INFO)
    logger.info(f'Starting {start_time.strftime("%m/%d/%Y %H:%M:%S")}')

    sleep_value = 30
    for i in range(1, 21):
        logger.info(f'Run - starting attempt {i}')
        try:
            success = run()
        except Exception:
            logger.error(traceback.format_exc())
            success = 0

        if(success):
            logger.info(f'Run - attempt {i} successful')
            break
        else:
            logger.info(f'Run - attempt {i} failed; sleeping for {sleep_value} seconds')
            sleep(sleep_value)
    
    if(not success):
        logger.info('Did not run successfully - sending failure notification')
        send_failure_email()

    end_time = datetime.now()
    
    logger.info(f'Finished {end_time.strftime("%m/%d/%Y %H:%M:%S")}, total runtime: {(end_time-start_time).total_seconds()} seconds')
