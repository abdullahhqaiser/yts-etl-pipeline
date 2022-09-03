import sys
sys.path.insert(0, '/opt/airflow/extra')
from misc import Helpers
from sql_queries import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging
import requests
import pyodbc
import ast



"""--------------------------------<FUNCTIONS>----------------------------------------"""


def _check_meta():
    logging.info("--------------------------CHECKING METADATA")
    meta_data = ast.literal_eval(Variable.get("meta_data", deserialize_json=False))

    if meta_data["track_ids"]:
        return 'load_new_movies'
    else:
        return 'load_movies'




def _load_movies():
    # grab page number from airflow backend
    meta_data = ast.literal_eval(Variable.get('meta_data', deserialize_json=False))
    api_url = Variable.get("api_url")
    page_number = meta_data["page_no"]

    page = requests.get(api_url.format(page_number)).json()

    if page_number == 1:

        Variable.set(key='meta_data', value={"track_ids": [page['data']['movies'][0]['id'],
                                                           page['data']['movies'][-1]['id']], "page_no": page_number})

    while True:
        if 'movies' in page['data'].keys():
            for movie in page['data']['movies']:
                _insert_movie(movie)

        page_number = page_number + 1
        page = requests.get(api_url.format(page_number)).json()

    ids = meta_data["track_ids"]
    Variable.set(key='meta_data', value={
                 "track_ids": ids, "page_no": page_number})


def _load_new_movies():
    meta_data = ast.literal_eval(Variable.get('meta_data', deserialize_json=False))
    logging.info("-----------------------loading new movies..")

    last_ids = meta_data['track_ids']
    page_number = 1
    api_url = Variable.get('api_url')
    page = requests.get(api_url.format(page_number)).json()

    Variable.set(key="meta_data", value={"track_ids": [page['data']['movies'][0]['id'],
                                                       page['data']['movies'][-1]['id']], "page_no": meta_data['page_no']})

    while True:
        current_ids = [page['data']['movies'][0]['id'],
                       page['data']['movies'][-1]['id']]

        if current_ids > last_ids:
            for movie in page['data']['movies']:
                print(f"---------------inserting {movie}")
                _insert_movie(movie)

        else:
            break

    _load_movies()


def _insert_movie(movie:dict):

    logging.info(f"inserting {movie}")
    hook = MsSqlHook(mssql_conn_id="azure_wh")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("select imdb_id from movies")

    if movie['imdb_code'] not in [i[0] for i in cur.fetchall()]:
        cur.execute(movie_table_insert, Helpers.validate(
                               movie, 'title'), Helpers.validate(movie, 'year'),
                           Helpers.validate(movie, 'rating'), Helpers.validate(
                               movie, 'runtime'), Helpers.validate(movie, 'mpa_rating'),
                           Helpers.validate(movie, 'language'), dt.strptime(
                               Helpers.validate(movie, 'date_uploaded'), '%Y-%m-%d %H:%M:%S'))

        if 'genres' in movie.keys():
                for genre in movie['genres']:
                    cur.execute(genre_moviegenre_insert, genre,
                                   genre, genre, movie['imdb_code'])

        if 'summary' in movie.keys():
                cur.execute(
                    summary_insert, movie['imdb_code'], movie['summary'])

    msg = str(movie['title'])
    print(f"{msg} INSERTED.")
        

        

"""--------------------------------<AIRFLOW CODE>----------------------------------------"""

default_args = {
    'owner': 'incoming_sign',
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

with DAG(dag_id='testing_41', start_date=datetime(2022, 9, 3), schedule_interval='@daily', default_args=default_args) as dag:
    check_meta = BranchPythonOperator(task_id = "check_meta", python_callable=_check_meta)

    load_movies = PythonOperator(task_id = "load_movies", python_callable=_load_movies)
    load_new_movies = PythonOperator(task_id = "load_new_movies", python_callable=_load_new_movies)

    check_meta >> [load_movies, load_new_movies]

    