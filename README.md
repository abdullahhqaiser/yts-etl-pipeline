
# YTS ETL PIPELINE

Building an airflow etl workflow that can get data from yts api and store data into azure sql db

## API Reference

#### Get movies 

```http
  GET https://yts.torrentbay.to/api/v2/list_movies.json?{page}
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `page` | `int` | **Required**. page number |

#### Get item

```http
  GET https://yts.torrentbay.to/api/v2/list_movies.json?limit{}
```

| Parameter | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `limit`      | `int` | **Required**. no of movies to fetch |



## Workflow Diagram

![Untitled Diagram drawio (1)](https://user-images.githubusercontent.com/88339218/189329045-4a85f53b-0a7b-4480-b6c0-ca30cbfddde9.png)

## Database Schema
For database, I used Azure SQL database with following schema

![image](https://user-images.githubusercontent.com/88339218/189409030-e8b1d370-1605-41e6-9db2-af8f364be49d.png)

## Run Locally
To run this project you need to have azure account and docker installed on your machine.

Clone the project

```bash
  git clone https://github.com/abdullah-raiwal/yts-etl-pipeline.git
```

Go to the project directory

```bash
  cd yts-etl-pipeline
```

Run docker on your local machine, then run this command in project directory
```bash
  docker build .
```

Finally build docker compose
```bash
  docker-compose up --airflow-init
```

Now launch airflow UI by following command
```bash
  docker-compose up
```
