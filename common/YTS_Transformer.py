import requests
from bs4 import BeautifulSoup
import pyodbc
import json
from common.Params import get_track_ids, update_track_ids


class etl():

    def __init__(self, conn_str: str, api_url: str, cast_url: str, params_path: str):
        conn = pyodbc.connect(self.conn_str)
        self.cursor = conn.Cursor()

        self.api_url = api_url
        self.cast_url = cast_url
        self.params_path = params_path
        self.track_ids = get_track_ids(self.params_path)

    def insert_movie(movie: dict, cursor: pyodbc.Cursor):

        # first, we need to extract current movie's cast list from imdb_id
        cast_list = get_cast(movie['imdb_code'])

        # now, let's insert data into respective tables one by one

        # 1-> Movie table
        cursor.execute("""
        insert into movies (imdb_id, title, year, rating, runtime, mpa_rating, language, date_uploaded)
        VALUES (?,?,?,?,?,?,?,?)
        """,
                       movie['imdb_code'], movie['title_long'], movie['year'], movie['rating'], movie[
                           'runtime'], movie['mpa_rating'], movie['language'], movie['date_uploaded']
                       )

        # 2-> genre and movie_genre table
        for genre in movie['genres']:
            cursor.execute("""
                        declare @temp_genre_id int;
                        if not exists (
                                        select * from genre
                                        where genre_title = ?
                                    )
                        begin
                            insert into genre values(?)
                        end
                select @temp_genre_id  = genre_id from genre where genre_title = ?
                insert into movie_genre
                values(?, @temp_genre_id)
            """, genre, genre, genre, movie['imdb_code'])

        # 3-> cast and movie_cast
        for actor in cast_list:
            cursor.execute("""
                        declare @temp_actor_id int;
                        if not exists (
                                        select * from cast
                                        where actor_name = ?
                                    )
                        begin
                                insert into cast values(?)
                        end
                select @temp_actor_id  = actor_id from cast where actor_name = ?
                insert into movie_cast
                values(?, @temp_actor_id)
            """, actor, actor, actor, movie_a['imdb_code'])

        cursor.execute("""

            insert into summary (imdb_id, summary)
            values (?, ?)

            """, movie['imdb_code'], movie['summary'])

    def get_cast(imdb_id: str):

        url = self.cast_url
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find('table', attrs={'class': 'cast_list'})
        cast = table.find_all('a')

        return re.findall(r'title="(.*?)"', str(cast))

    def insert_new_movies():

        page_number = 1
        new_insertion = 0
        while True:
            init_page = requests.get(self.api_url).json()

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
                update_track_ids(self.params_path, current_ids)
                break

    def load():
        # this method going to to load all data from api, and if called other time, this method gonna only insert new data..

        page_number = 1

        while True:
            page = requests.get(self.api_url).json()
            
            if page_number == 1:
                ids = {'track_id': [page['data']['movies']
                                    [0], page['data']['movies'][-1]]}

            if 'movies' in page['data'].keys():
                for movie in page['data']['movies']:
                    self.insert_movie(movie, self.cursor)

                page_number = page_number + 1

        self.cursor.close()
