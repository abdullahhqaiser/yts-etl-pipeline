
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



## Screenshots

![App Screenshot](https://via.placeholder.com/468x300?text=App+Screenshot+Here)

