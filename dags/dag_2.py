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
import time

hook = MsSqlHook(mssql_conn_id="azure_wh", supports_autocommit= True)
conn = hook.get_conn()

"""--------------------------------<FUNCTIONS>----------------------------------------"""


def _check_meta():
    logging.info("--------------------------CHECKING METADATA")
    meta_data = ast.literal_eval(Variable.get("meta_data", deserialize_json=False))

    if meta_data['track_ids']:
        return 'load_new_movies'
    else:
        return 'load_movies'



def _load_movies():

    # grab page number from airflow backend 
    meta_data = ast.literal_eval(Variable.get('meta_data', deserialize_json=False))
    page_number = meta_data['page_no']
    api_url = Variable.get("api_url")
    ids = meta_data["track_ids"]

    logging.info("-----------------------loading movies..")

    page = requests.get(api_url.format(page_number)).json()
    if page_number == 1:
        ids = [page['data']['movies'][0]['id'], page['data']['movies'][-1]['id']]
        Variable.set(key='meta_data', value={"track_ids" : ids, "page_no" : page_number})


    while 'movies' in page['data'].keys():
        logging.info(f"current page {page_number}")
        logging.info(f"IDS = {[page['data']['movies'][0]['id'], page['data']['movies'][-1]['id']]}")

        for movie in page['data']['movies']:
            _insert_movie(movie, conn)

        conn.commit()
        Variable.set(key = "meta_data", value ={"track_ids": ids, "page_no" : page_number})
        page_number = page_number + 1
        page = requests.get(api_url.format(page_number)).json()
  


def _load_new_movies():

    meta_data = ast.literal_eval(Variable.get('meta_data', deserialize_json=False))
    last_ids = meta_data['track_ids']
    page_number = 1
    api_url = Variable.get('api_url')
    page = requests.get(api_url.format(page_number)).json()
    current_ids = [page['data']['movies'][0]['id'], page['data']['movies'][-1]['id']]

    logging.info("-----------------------loading new movies..")

    Variable.set(key="meta_data", value={"track_ids": [page['data']['movies'][0]['id'],
                                                       page['data']['movies'][-1]['id']], "page_no": meta_data['page_no']})

    
    while current_ids > last_ids:

        logging.info(f"current page {page_number}")
        logging.info(f"IDS = {page['data']['movies'][0]['id']}, {page['data']['movies'][-1]['id']}")
        for movie in page['data']['movies']:
            _insert_movie(movie, conn)

        conn.commit()
        page_number = page_number + 1
        page = requests.get(api_url.format(page_number)).json()
        current_ids = [page['data']['movies'][0]['id'], page['data']['movies'][-1]['id']]

    _load_movies()


def _insert_movie(movie:dict, conn):

    cur = conn.cursor()
    msg = movie['title']    

    cur.execute("select imdb_id from movies")

    if movie['imdb_code'] not in [i[0] for i in cur.fetchall()]:
        cur = conn.cursor()

        cur.execute(movie_table_insert,(Helpers.validate(movie, 'imdb_code'), Helpers.validate(
                               movie, 'title'), Helpers.validate(movie, 'year'),
                           Helpers.validate(movie, 'rating'), Helpers.validate(
                               movie, 'runtime'), Helpers.validate(movie, 'mpa_rating'),
                           Helpers.validate(movie, 'language'), datetime.strptime(Helpers.validate(movie, 'date_uploaded'), '%Y-%m-%d %H:%M:%S')))

        if 'genres' in movie.keys():

                for genre in movie['genres']:
                    cur.execute(genre_moviegenre_insert, (genre,
                                   genre, genre, movie['imdb_code']))

        if 'summary' in movie.keys():

                cur.execute(
                    summary_insert, (movie['imdb_code'], movie['summary']))    
        logging.info(f"inserted {msg}")
    else:
        logging.info(f"skipped {msg}")



"""--------------------------------<AIRFLOW CODE>----------------------------------------"""

default_args = {
    'owner': 'incoming_sign',
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

with DAG(dag_id='testing_72', start_date=datetime(2022, 9, 5), schedule_interval='@daily', default_args=default_args) as dag:
    check_meta = BranchPythonOperator(task_id = "check_meta", python_callable=_check_meta)

    load_movies = PythonOperator(task_id = "load_movies", python_callable=_load_movies)
    load_new_movies = PythonOperator(task_id = "load_new_movies", python_callable=_load_new_movies)

    check_meta >> [load_movies, load_new_movies]


    