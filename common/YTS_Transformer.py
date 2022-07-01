import requests
from bs4 import BeautifulSoup


class etl():

    def load_movie(movie: dict, cursor: pyodbc.Cursor):

        def get_pages():
            pass

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

        url = f'https://www.imdb.com/title/{imdb_id}/fullcredits'
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find('table', attrs={'class': 'cast_list'})
        cast = table.find_all('a')

        return re.findall(r'title="(.*?)"', str(cast))
