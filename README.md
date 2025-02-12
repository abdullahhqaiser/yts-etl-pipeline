# Automated Movie Data ETL Pipeline Using Apache Airflow

## Project Description  
This project is a robust ETL (Extract, Transform, Load) pipeline designed to automate the ingestion and management of movie data using Apache Airflow. The pipeline integrates with an external API to fetch movie information and loads it into a Microsoft SQL Server database. It is built to handle both new and existing movie data dynamically, ensuring efficient data tracking and updates.

---

## Key Features
1. **Dynamic Metadata Management**  
   - Uses Airflow Variables to manage metadata, including tracking the last processed movie IDs and the current API page number.  
   - Dynamically determines whether to fetch all movies or only the new ones based on metadata comparison.

2. **Data Extraction**  
   - Fetches movie data from an external API, paginated for efficient data transfer.  
   - Ensures only relevant data is processed, reducing unnecessary computation and API calls.

3. **Data Transformation and Validation**  
   - Utilizes helper functions to validate and format movie attributes like title, year, genre, and ratings.  
   - Processes nested structures like genres and summaries to ensure comprehensive data loading.

4. **Data Loading**  
   - Inserts movies into a relational database (Microsoft SQL Server) using pre-defined SQL queries.  
   - Prevents duplicate entries by verifying data integrity before insertion.

5. **Branching and Task Management**  
   - Implements branching logic using `BranchPythonOperator` to switch between full data reloads and incremental updates based on the state of the metadata.  
   - Modular tasks ensure flexibility and scalability.

6. **Error Handling and Logging**  
   - Comprehensive logging to monitor task execution, data insertion, and potential issues.  
   - Commits transactions incrementally for data safety and recovery.

---

## Technologies Used
- **Apache Airflow**: Orchestration and workflow management.  
- **Python**: Core programming language for custom tasks and API interactions.  
- **Microsoft SQL Server**: Relational database for structured data storage.  
- **API Integration**: Dynamic interaction with external movie data APIs.  

---

## Workflow Overview
1. **Metadata Check**  
   The pipeline starts with a metadata check to decide the operational path: load all movies or only new ones.  

2. **Data Loading Tasks**  
   - **Load Movies Task**: Handles complete data ingestion when no previous metadata exists.  
   - **Load New Movies Task**: Fetches only newly available data, optimizing performance.  

3. **Database Updates**  
   Inserts movie details, genres, and summaries into corresponding database tables with necessary validations and transformations.  

---

## Real-World Application  
This ETL pipeline is ideal for organizations managing large movie datasets, such as streaming platforms or entertainment analytics companies. It provides a scalable and efficient mechanism for continuous data ingestion and database updates, reducing manual intervention.

---

## How to Use
1. Clone the repository to your local machine.  
2. Configure the Airflow environment, including the `mssql_conn_id` and required variables in the Airflow UI.  
3. Set the `api_url` and `meta_data` variables in Airflow.  
4. Trigger the DAG `testing_72` to start the workflow.

---

Feel free to reach out with questions or feedback!

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
