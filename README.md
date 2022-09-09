
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
