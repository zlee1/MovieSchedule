from time import sleep
import random
from datetime import datetime
from datetime import timedelta
import re
import traceback
import pandas as pd
import sqlite3
import platform
import os
import subprocess

import warnings
warnings.filterwarnings("ignore")

from bs4 import BeautifulSoup

import urllib.request
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
# from subprocess import CREATE_NO_WINDOW



def browser_init():
    """Create Selenium browser instance.
    
    Returns:
    selenium browser instance
    """

    options = Options()

    options.add_argument("--headless") # run browser without opening window
    options.add_argument("--log-level=3") # log only errors

    if(platform.system() == 'Linux'):
        service = Service(executable_path='/snap/bin/geckodriver')
    else:
        service = Service()
        
    # service.creationflags = CREATE_NO_WINDOW # fully suppress selenium logging

    driver = webdriver.Firefox(options=options, service=service)
    driver.set_page_load_timeout(300)
    
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

def get_zip_codes(from_file=1, filename=None):
    """Get list of zip codes to be used for locating theaters. 

    Keyword Arguments:
    from_file - whether to read zip codes from file (1) or take user input (0)
    filename - relative path to read zip codes from when from_file == 1

    Returns:
    list - [str zip code, str zip code, str zip code]
    """

    if(filename is None):
        filename = ('\\' if platform.system() == 'Windows' else '/').join(['data', 'data.txt'])

    if(not from_file):
        zip_codes = []
        zip_code = None
        while zip_code != '':
            zip_code = str(input('Enter a zip code (empty for done): ')).strip()
            if(zip_code != ''):
                zip_codes.append(zip_code)
        print(f'zip codes: {zip_codes}')
        return zip_codes
    else:
        with open(filename, 'r') as f:
            for i in f.readlines():
                if(i[:3] == 'zip'):
                    zip_codes = re.sub('zip=', '', i).replace('\n', '').split(',')
                    print(f'zip codes: {zip_codes}')
                    return zip_codes

def get_soup(theater, url, date, browser):
    formatted_date = date.strftime('%Y-%m-%d')

    full_url = f'{url}?cmp=theater-module&format=all&date={formatted_date}'
    
    print(f'current theater: {theater}\ncurrent date: {date}\naddress: {full_url}')
    
    for i in range(10):

        browser.get(full_url)

        sleep(random.randint(10, 45))

        soup = BeautifulSoup(browser.page_source, 'html.parser')

        container = soup.find('h1', 'offline__header')
        if(container is None):
            break;
        else:
            print('offline')
            sleep(60)
    return soup

def get_text(soup):
    return soup.text.replace('\n', '').replace('\t', '').replace('\'', '\'\'').strip()

def select_all_from_table(tablename, conn):
    return pd.read_sql_query(f'SELECT * FROM {tablename}', conn)

def get_theaters(zip_codes):
    """Get list of all theaters that appear in search for each provided zip code.

    Keyword arguments:
    zip_codes - list of zip codes as string

    Returns:
    list - [ { id : theater id , name : theater name , url : theater url } ]
    """

    theater_list = [] # theater name : theater page url
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
    print(theater_list)
    return theater_list

def insert_theaters(theaters, cursor):
    for theater in theaters:
        query = f"""
        INSERT INTO theaters(id, name, url, address)
        VALUES(
            \'{theater.get('id')}\'
            ,\'{theater.get('name')}\'
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

def get_movies_from_theater(soup):
    movies = []
    
    container = soup.find('ul', 'thtr-mv-list')

    if(container is None):
        print('no movies found')
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
            print(f'no image found for movie {movie_id}')
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
            print(f'year not found for {movie_name}')
        
        if(movie_year is not None):
            movie_name = movie_name[:-7]

        movie_url = 'https://www.fandango.com' + title_sect.find('a')['href']

        movie_info_sect = detail_sect.find('li')
        info_text = get_text(movie_info_sect)
        try:
            movie_rating = info_text.split(', ')[0]
        except Exception as e:
            movie_rating = None
            print(f'{e}, error with parsing rating')
        
        try:
            if('min' not in info_text.split(', ')[1]):
                movie_runtime = int(info_text.split(', ')[1].replace(' ', '').replace('hr', ''))*60
                print('hr only runtime: ', movie_runtime)
            else:
                raw_runtime = info_text.split(', ')[1].replace(' min', '').replace(' ', '').split('hr')

                movie_runtime = int(raw_runtime[0])*60 + int(raw_runtime[1])
        except Exception as e:
            movie_runtime = None
            print(f'{e}, error with parsing runtime from {info_text}')
        
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
        print('no showtimes found')
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

def get_all_movies_and_showtimes(theaters, dates, browser):
    movies = []
    showtimes = []
    for index, row in theaters.iterrows():
        for date in dates:
            soup = get_soup(row['name'], row['url'], date, browser)
            
            movies += get_movies_from_theater(soup)
            showtimes += get_showtimes_from_theater(soup)
    
    return movies, showtimes

def insert_movies(movies, cursor):
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

def insert_showtimes(showtimes, cursor):
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
            WHERE url = excluded.url
        ;
        """

        cursor.execute(query)




if __name__ == '__main__':
    try:

        driver = browser_init()


        conn, cursor = initialize_db(('\\' if platform.system() == 'Windows' else '/').join(['sqlite3', 'moviedb']))

        zip_codes = get_zip_codes(from_file=1)
        theaters = get_theaters(zip_codes)

        insert_theaters(theaters, cursor)

        theater_df = select_all_from_table('theaters', conn)

        movies, showtimes = get_all_movies_and_showtimes(theater_df, [datetime.now().date() + timedelta(days=i) for i in range(7)], driver)

        print(movies)

        insert_movies(movies, cursor)

        insert_showtimes(showtimes, cursor)

        movie_df = select_all_from_table('movies', conn)

        showtime_df = select_all_from_table('showtimes', conn)

        print(theater_df)
        print('\n')
        print(movie_df)
        print('\n')
        print(showtime_df)
        print('\n')

        conn.commit()

    except Exception:
        print(traceback.format_exc())
    finally:
        conn.close()
        driver.close()