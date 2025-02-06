from time import sleep
import random
from datetime import datetime
from datetime import timedelta
import re
import traceback
import pandas as pd
import sqlite3
import logging
import os
import sys
import subprocess
from duckdb import sql
import requests
import unicodedata

import warnings
warnings.filterwarnings("ignore") # warnings are annoying!

from bs4 import BeautifulSoup

import urllib.request
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
# from subprocess import CREATE_NO_WINDOW

logger = logging.getLogger('data_collection')

# urllib_sleep = 15 # time to sleep when collecting theater data from zip codes
sleep_amt = 30 # time to sleep when collecting showtime and movie data from theaters

log_location = None # filepath for log
driver_location = None # filepath for chrome driver
app_db = None # filepath for webapp database - needed for subscription data

progress_made = False # bool to keep track of whether any progess was made in a run

collected_movies = []

def browser_init(headless=False):
    """Create Selenium browser instance.
    
    Returns:
    selenium browser instance
    """
    logger.info('Creating new browser instance')

    options = Options()

    if(headless):
        logger.info('Running in headless mode')
        options.add_argument("--headless=new") # run browser without opening window

    options.add_argument("--no-sandbox") #bypass OS security model
    options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems

    # options.add_argument("--log-level=3") # log only errors
    options.add_argument('--blink-settings=imagesEnabled=false') # prevent image loading

    service = Service(executable_path=driver_location)
    
    # service.creationflags = CREATE_NO_WINDOW # fully suppress selenium logging

    driver = webdriver.Chrome(service=service, options=options)
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

    # zip codes to check are those with active subscriptions
    return list(pd.read_sql('SELECT DISTINCT zip_code FROM subscriptions WHERE active=1;', conn)['zip_code'])

def get_soup(theater, url, date, browser):
    """Get BeautifulSoup object of a page

    Keyword Arguments:
    theater - name of theater
    url - base url of theater (https://www.fandango.com/shu-community-theatre-aabqu/theater-page)
    date - date of showings to collect
    browser - selenium browser

    Returns:
    BeautifulSoup object
    """

    # date must be in YYYY-mm-dd format for url
    formatted_date = date.strftime('%Y-%m-%d')

    # full url incorporates date restriction
    full_url = f'{url}?cmp=theater-module&format=all&date={formatted_date}'
    
    logger.info(f'current theater: {theater} | current date: {date} | address: {full_url}')
    
    # try to get html until page loads properly - max 10 attempts
    for i in range(10):

        browser.get(full_url)

        sleep(random.randint(sleep_amt//2, sleep_amt)) # wait time incorporated so my ip doesn't get banned again

        soup = BeautifulSoup(browser.page_source, 'html.parser')

        # if offline__header exists, page hasn't loaded properly
        container = soup.find('h1', 'offline__header')
        if(container is None):
            break;
        else:
            logger.warning('offline')
            sleep(60)
    return soup

def get_text(soup):
    """Get text from a BeautifulSoup element

    Keyword arguments:
    soup - BeautifulSoup element

    Returns:
    str - cleaned text of element
    """

    return soup.text.replace('\n', '').replace('\t', '').replace('\'', '\'\'').strip()

def select_all_from_table(tablename, conn):
    """Select all data from a given table in database

    Keyword arguments:
    tablename - name of table in database to query
    conn - database connection

    Returns:
    pd.DataFrame - all data from table
    """

    return pd.read_sql_query(f'SELECT * FROM {tablename}', conn)

def get_subscriptions():
    """Get subscription and theater data from django app database
    """

    logger.info('Connecting to django app database')
    conn, cursor = initialize_db(app_db)

    logger.info('Collecting subscription data')
    active_users = pd.read_sql('SELECT id, username, first_name, last_name, email FROM auth_user WHERE is_active=1', conn)
    all_theaters = pd.read_sql('SELECT id, name, url FROM subscriptions_theater WHERE id IN (SELECT DISTINCT theater_id FROM subscriptions_subscription)', conn)
    all_subscriptions = pd.read_sql('SELECT user_id, theater_id FROM subscriptions_subscription', conn)

    # take only active data
    subscriptions = sql('SELECT s.user_id, s.theater_id FROM all_subscriptions s INNER JOIN active_users u ON u.id = s.user_id').df()
    theaters = sql('SELECT t.id, t.name, t.url FROM all_theaters t INNER JOIN subscriptions s ON s.theater_id = t.id').df()
    
    logger.info('Subscription data colelcted - closing database connection')
    conn.close()

    return subscriptions, theaters

def get_subscriptions_api():
    """Get subscription and theater data from django app api
    """

    logger.info('Collecting subscription data from API')
    all_subscriptions = pd.DataFrame(requests.get(os.environ['WEBAPP_BASEURL'] + 'api/subscriptions/', headers={'Authorization': f'Token {os.environ["API_KEY"]}'}).json())
    
    logger.info('Collecting theater data from API')
    all_theaters = pd.DataFrame(requests.get(os.environ['WEBAPP_BASEURL'] + 'api/theaters/', headers={'Authorization': f'Token {os.environ["API_KEY"]}'}).json())

    logger.info('Collecting user data from API')
    all_users = pd.DataFrame(requests.get(os.environ['WEBAPP_BASEURL'] + 'api/users/', headers={'Authorization': f'Token {os.environ["API_KEY"]}'}).json())

    logger.info('Restricting data to active only')

    active_users = sql('SELECT * FROM all_users WHERE is_active=1').df()
    active_subscriptions = sql('SELECT * FROM all_subscriptions WHERE user_id IN (SELECT DISTINCT id FROM active_users)').df()
    active_theaters = sql('SELECT * FROM all_theaters WHERE id IN (SELECT DISTINCT theater_id FROM active_subscriptions)').df()

    logger.info('API data collected')
    return active_subscriptions, active_theaters


def insert_zip_code(zip_code, theater_id, cursor):
    """Insert zip code data into database
    
    Keyword arguments:
    zip_code - zip code as string
    theater_id - id of theater as string
    cursor - cursor for database
    
    Returns:
    None
    """

    query = f"""
        INSERT OR IGNORE INTO zip_codes(zip_code, theater_id)
        VALUES(
            \'{zip_code}\'
            ,\'{theater_id}\'
        );
        """
    
    cursor.execute(query)

def collect_theaters(zip_codes, conn, cursor):
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

        sleep(sleep_amt)

        zip_search.close()

        theaters = zip_search_page.find(id='nearby-theaters-select-list').find_all('option') # list of all theaters on page

        for theater in theaters[1:]:
            theater_dict = {}
            theater_dict['id'] = theater["value"].replace('/', '').replace('theater-page', '')[-5:] # end of url is theater id
            theater_dict['name'] = theater.text.strip()
            theater_dict['url'] = f'https://www.fandango.com{theater["value"]}'
            theater_dict['address'] = None # to be added later? probably not, doesn't seem especially useful and would require visiting each theater page

            if(theater_dict not in theater_list):
                theater_list.append(theater_dict)

            # insert data into zip code table
            logger.info(f'Adding {theater_dict["name"]} for zip code {zip_code}')
            insert_zip_code(zip_code, theater_dict['id'], cursor)
    
    # insert data into theater table
    logger.info('Inserting theater data')
    insert_theaters(theater_list, conn, cursor)

def insert_theaters(theaters, conn, cursor):
    # loop through all theaters
    for index, row in theaters.iterrows():
        query = f"""
        INSERT INTO theaters(id, name, url, address)
        VALUES(
            \'{row['id']}\'
            ,\'{row['name'].replace('\'', '\'\'')}\'
            ,\'{row['url'] if row['url'] != None else ''}\'
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
    global progress_made
    progress_made = True

def collect_movies_from_theater(soup, browser):
    global collected_movies
    movies = []
    
    container = soup.find('ul', 'thtr-mv-list')

    if(container is None):
        logger.warning('no movies found')
        return movies

    for movie in container.find_all('li'):
        if(not movie.parent.__eq__(container)): # list item must be direct child of container
            continue;

        movie_id = movie['id'].replace('movie-', '')
        if(movie_id in collected_movies):
            continue;
        else:
            collected_movies.append(movie_id)

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
            if(re.match(r'\([0-9]{4}\)', get_text(title_sect)[-6:])):
                movie_year = int(get_text(title_sect)[-6:].replace('(', '').replace(')', ''))
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

        movie_info = get_movie_info(movie_url, browser)
        
        movie_dict = {
                'id': movie_id
                ,'name': movie_name
                ,'url': movie_url
                ,'release_year': movie_year
                ,'runtime': movie_runtime
                ,'rating': movie_rating
                ,'image_url': movie_image_url
        }
        movie_dict.update(movie_info)

        movies.append(movie_dict)
        
    return movies

def collect_showtimes_from_theater(soup):
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

def collect_all_movies_and_showtimes(theaters, dates, browser, conn, cursor, redo=False):
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
            if(row['date_updated'] is not None and date <= datetime.strptime(row['date_updated'], '%Y-%m-%d').date()+timedelta(days=6)):
                logger.info(f'Skipping date {datetime.strftime(date, "%Y-%m-%d")} for theater {row["name"]} - data already collected.')
                continue
            soup = get_soup(row['name'], row['url'], date, browser)
            
            new_movies += collect_movies_from_theater(soup, browser)
            new_showtimes += collect_showtimes_from_theater(soup)
        
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

def get_movie_info(url, browser):
    logger.info(f'Collecting movie info at {url}')
    page = browser.get(url)
    movie = BeautifulSoup(browser.page_source, 'html.parser')

    sleep(sleep_amt)

    ratings = movie.findAll('span', 'rottentomatoes-rating')
    if(len(ratings) == 2):
        rt_critic = int(''.join([i if i.isdigit() else '' for i in get_text(ratings[0])]))
        rt_audience = int(''.join([i if i.isdigit() else '' for i in get_text(ratings[1])]))
    else:
        rt_critic, rt_audience = None, None

    genre_container = movie.find('li', 'movie-detail__grv-item')
    if(genre_container and 'GENRE' in get_text(genre_container)):
        genres = ', '.join([i.strip() for i in unicodedata.normalize('NFKD', get_text(genre_container).replace('GENRE:', '').strip()).split(',')])
    else:
        genres = None

    synopsis_container = movie.find(id='movie-detail-synopsis')
    if(synopsis_container):
        synopsis = get_text(synopsis_container)
        if(synopsis == ''):
            synopsis = None
    else:
        synopsis = None

    return {'rt_critic': rt_critic, 'rt_audience': rt_audience, 'genres': genres, 'synopsis': synopsis}

def theater_date_update(theater_id, conn, cursor):
    cursor.execute(f"UPDATE theaters SET date_updated = CURRENT_DATE WHERE id=\'{theater_id}\';")
    conn.commit()
    global progress_made
    progress_made = True

def insert_movies(movies, conn, cursor):
    logger.info(f'Inserting {len(movies)} movies')
    for movie in movies:
        query = f"""
        INSERT INTO movies(id, name, url, release_year, runtime, rating, image_url, rt_critic, rt_audience, genres, synopsis)
        VALUES(
            \'{movie.get('id')}\'
            ,\'{movie.get('name')}\'
            ,\'{movie.get('url')}\'
            ,{movie.get('release_year') if movie.get('release_year') != None else 'NULL'}
            ,{movie.get('runtime') if movie.get('runtime') != None else 'NULL'}
            ,\'{movie.get('rating') if movie.get('rating') != None else ''}\'
            ,\'{movie.get('image_url') if movie.get('image_url') != None else ''}\'
            ,\'{movie.get('rt_critic') if movie.get('rt_critic') != None else 'NULL'}\'
            ,\'{movie.get('rt_audience') if movie.get('rt_audience') != None else 'NULL'}\'
            ,\'{movie.get('genres') if movie.get('genres') != None else ''}\'
            ,\'{movie.get('synopsis') if movie.get('synopsis') != None else ''}\'
        )
        ON CONFLICT(id) DO UPDATE SET
            name = COALESCE(excluded.name, name)
            ,release_year = COALESCE(excluded.release_year, release_year)
            ,runtime = COALESCE(excluded.runtime, runtime)
            ,rating = COALESCE(excluded.rating, rating)
            ,image_url = COALESCE(excluded.image_url, image_url)
            ,rt_critic = COALESCE(excluded.rt_critic, rt_critic)
            ,rt_audience = COALESCE(excluded.rt_audience, rt_audience)
            ,genres = COALESCE(excluded.genres, genres)
            ,synopsis = COALESCE(excluded.synopsis, synopsis)
            WHERE url = excluded.url
        ;
        """

        cursor.execute(query)
        
    conn.commit()
    global progress_made
    progress_made = True

def insert_showtimes(showtimes, conn, cursor):
    logger.info(f'Inserting {len(showtimes)} showtimes')
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
    global progress_made
    progress_made = True

def collect_data(headless = False):
    conn = None
    driver = None

    try:
        logger.info('Initializing browser')
        driver = browser_init(headless)

        logger.info('Connecting to database')
        conn, cursor = initialize_db(os.path.join('sqlite3', 'moviedb'))

        # zip_codes = get_zip_codes(conn)
        
        # logger.info('Collecting theaters')
        # collect_theaters(zip_codes, conn, cursor)

        # zip_code_str = ','.join([f"\'{i}\'" for i in zip_codes])

        app_subscriber_df, app_theater_df = get_subscriptions_api()
        insert_theaters(app_theater_df, conn, cursor)

        theater_ids = list(sql('SELECT DISTINCT id FROM app_theater_df').df()['id'])

        theater_df = pd.read_sql(f"SELECT * FROM theaters WHERE id IN ({','.join(['\''+id+'\'' for id in theater_ids])})", conn)

        logger.info('Collecting movies and showtimes')
        collect_all_movies_and_showtimes(theater_df, [datetime.now().date() + timedelta(days=i) for i in range(7)], driver, conn, cursor, redo=False)

    except Exception:
        logging.error(traceback.format_exc())
        success = 0
    else:
        success = 1
    finally:
        try: 
            conn.close() 
        except: 
            logger.error('Attempted to close non-existent database connection')

        try: 
            driver.close()
        except: 
            logger.error('Attempted to close non-existent webdriver')

        try:
            subprocess.call(['sudo', 'protonvpn', 'd'])
        except:
            logger.error('Attempted to end non-existent vpn session')

        logger.info('Closed db connection and webdriver')
        return success

def run(vpn=True, headless=False):

    global logger
    global log_location
    global driver_location
    global app_db
    global progress_made

    start_time = datetime.now()

    with open(os.path.join('data', 'file_locations.txt'), 'r') as f:
        file_locations = f.read().splitlines()

    for i in file_locations:
        if(i.startswith('log=')):
            log_location = i.split('log=')[1]
        elif(i.startswith('driver=')):
            driver_location = i.split('driver=')[1]
        elif(i.startswith('app_db=')):
            app_db = i.split('app_db=')[1]

    log_location = os.path.join('logs', f'movie_schedule_{datetime.now().strftime("%d%m%Y")}.log')
    if(not os.path.isfile(log_location)):
        open(log_location, 'w+')
    else:
        with open(log_location, 'a') as f:
            f.write('\n\n\n')

    if(driver_location is None):
        raise Exception('WebDriver not provided. Please add WebDriver filepath to data/file_locations.txt on a new line in the format of "driver=<filepath>"')

    logging.basicConfig(filename=log_location, level=logging.INFO)
    logger.info(f'Starting {start_time.strftime("%m/%d/%Y %H:%M:%S")}')

    if(vpn):
        logger.info(f'Starting VPN')

        subprocess.call(['sudo', 'protonvpn', 'c', '-f'])

        sleep(30)


    sleep_value = 300

    success = 0
    no_progress_ct = 0
    runs = 0
    while not success and no_progress_ct <= 10:
        runs += 1
        logger.info(f'Run - starting attempt {runs}')
        try:
            success = collect_data(headless)
        except Exception:
            logger.error(traceback.format_exc())
            success = 0

        if(success):
            logger.info(f'Run - attempt {runs} successful')
            break
        else:
            logger.info(f'Run - attempt {runs} failed; sleeping for {sleep_value} seconds')
            if(progress_made):
                no_progress_ct = 0
                progress_made = False
            else:
                no_progress_ct += 1
                logger.warning(f'No progress made in run {runs} - number of consecutive runs without progress is now {no_progress_ct}')
            sleep(sleep_value)

    end_time = datetime.now()

    if(vpn):
        logger.info('Ending vpn session')

        subprocess.call(['sudo', 'protonvpn', 'd'])
    
    logger.info(f'Finished {end_time.strftime("%m/%d/%Y %H:%M:%S")}, total runtime: {(end_time-start_time).total_seconds()} seconds')

    return success

if __name__ == "__main__":
    run(headless = 1 if 'headless' in sys.argv else 0)
