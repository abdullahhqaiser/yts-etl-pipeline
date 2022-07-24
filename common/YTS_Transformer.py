import requests
from bs4 import BeautifulSoup
import pyodbc
import json
from common.misc import ProcessParams
import logging
from sql_scripts.sql_queries import *
import re
from datetime import datetime as dt
import pandas as pd


class etl():

    def __init__(self, source_config: dict, tracking_config: dict, destination_config: dict):

        self.conn = pyodbc.connect(
            'Driver={SQL Server};''Server=DESKTOP-K8VV6KV;''Database=yts_warehouse;''Trusted_Connection=yes;''autocommit=True')
        self.cursor = self.conn.cursor()

        # loading source configs
        self.api_url = source_config['api_url']
        # self.cast_url = source_config['cast_url']

        # loading tracking configs
        self.meta_path = tracking_config['meta_path']

        self.conn_str = destination_config['dest_conn_str']

        self._logger = logging.getLogger(__name__)

        self._logger.info('created ETL class')
        self.track_ids = ProcessParams.get_track_ids(self.meta_path)
        

    def insert_movie(self, movie: dict, cursor: pyodbc.Cursor):

        # first, we need to extract current movie's cast list from imdb_id
        # cast_list = self.get_cast(movie['imdb_code'])

        # now, let's insert data into respective tables one by one

        # but before that, we need to check whether that movie exists in db or not,
        if movie['imdb_code'] not in [i[0] for i in cursor.execute("select imdb_id from movies")]:

            # 1-> Movie table
            cursor.execute(movie_table_insert,
                           ProcessParams.validate(movie, 'imdb_code'), ProcessParams.validate(
                               movie, 'title'), ProcessParams.validate(movie, 'year'),
                           ProcessParams.validate(movie, 'rating'), ProcessParams.validate(
                               movie, 'runtime'), ProcessParams.validate(movie, 'mpa_rating'),
                           ProcessParams.validate(movie, 'language'), dt.strptime(
                               ProcessParams.validate(movie, 'date_uploaded'), '%Y-%m-%d %H:%M:%S')
                           )

            # 2-> genre and movie_genre table
            if 'genres' in movie.keys():
                for genre in movie['genres']:
                    cursor.execute(genre_moviegenre_insert, genre,
                                   genre, genre, movie['imdb_code'])

            # # 3-> cast and movie_cast
            # for actor in cast_list:
            #     cursor.execute(cast_moviecast_insert, actor,
            #                    actor, actor, movie['imdb_code'])
            if 'summary' in movie.keys():
                cursor.execute(
                    summary_insert, movie['imdb_code'], movie['summary'])

            msg = str(movie['title'])
            self._logger.info(f" {msg} INSERTED!")
        # else:
        #     self._logger.info(
        #         f" {movie['title_long']} DUPLICATE FOUND, IGNORING!")

    # def get_cast(self, imdb_id: str):

    #     url = self.cast_url
    #     page = requests.get(self.cast_url.format(imdb_id))
    #     soup = BeautifulSoup(page.content, 'html.parser')
    #     table = soup.find('table', attrs={'class': 'cast_list'})
    #     cast = table.find_all('a')

    #     return re.findall(r'title="(.*?)"', str(cast))

    def insert_new_movies(self):

        with open('../' + self.meta_path) as f:
            meta_file = json.load(f)
        


        page_number = meta_file['page_number']
        new_insertion = 0
        self._logger.info(" Preodic ETL Job run. Loading New movies.")

        while True: 
            init_page = requests.get(self.api_url.format(page_number)).json()

            # track_ids of current page or initial page.
            current_ids = [init_page['data']['movies'][0]
                           ['id'], init_page['data']['movies'][-1]['id']]
            # check if current ids are greater than ids that are stored in
            # params.yaml file
            if current_ids > self.track_ids:
                new_insertion = new_insertion+len(init_page['data']['movies'])
                for movie in init_page['data']['movies']:
                    self.insert_movie(movie, self.cursor)
                # after all movies inserted in db, update track_ids
                self.track_id = current_ids
                page_number = page_number + 1
            else:
                ProcessParams.update_track_ids(self.params_path, current_ids)
                self.cursor.close()
                break

    def load(self):
        # this method going to to load all data from api, and if called other time, this method gonna only insert new data..
        # first check if there are any ids in params file, if so, then we need to insert new movies

        if self.track_ids:
            self.insert_new_movies()

        else:
            page_number = 1
            self._logger.info("Initial ETL Job Run. Loading all movies")
            while True:
                self._logger.info(f" current page is {page_number}")
                page = requests.get(self.api_url.format(page_number)).json()

                if page_number == 1:
                    ids = {'track_id': [page['data']['movies']
                                        [0], page['data']['movies'][-1]]}

                if 'movies' in page['data'].keys():
                    for movie in page['data']['movies']:
                        self.insert_movie(movie, self.cursor)

                    page_number = page_number + 1
                    self.conn.commit()

            self._logger.info(" Saving meta file...")
            ProcessParams.update_track_ids(self.params_path, ids)

            self.cursor.close()
            self._logger('ETL Job Finished.')
