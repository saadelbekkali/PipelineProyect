# Data Warehouse Pipeline Project

## Overview

This project implements a data pipeline for loading, transforming, and validating sales data into a DuckDB warehouse. It utilizes a multi-layer architecture consisting of Bronze, Silver, and Gold data layers, with data quality checks integrated into the process.

## Project Structure


project_root/ 

 ├── data/ │

    ├── bronze/ # Raw data │ 

    ├── silver/ # Transformed data │

    ├── gold/ # Data warehouse │ 

 ├── logs/ # Log files for different processes 

 ├── src/ # Source code

        ├── datageneration.py # Data generation script 

        ├── ingestion.py # Data ingestion script 

        ├── main_pipeline.py # Main pipeline orchestration 

        ├── quality_checks.py # Data quality checks 

        ├── transform.py # Data transformation script

        └── warehouse_load.py # Warehouse loading script

  ├── AccessDW    # Access and queries for the Data Warehouse

  ├── Dockerfile    # Dockerfile

  └── requirements.txt # Python dependencies


## How to Run the Code in your local

1. **Install Requirements**: 
   Before running the project, make sure to install the required packages. You can do this by executing:
   
```
   pip install -r requirements.txt
```

2. **Run the Main Pipeline**: 
Execute the main pipeline to start the data processing workflow:
   
```
   python src/main_pipeline.py
```

3. **Query the Data Warehouse**: 
Use the AccessDW script to run queries on the created DuckDB database. It will check if the sales_data table exists and will execute a query to fetch results:
  
```  
     python AccessDW
```

## Challenges Faced

One of the main challenges was structuring the logs correctly so that each component of the pipeline had its own log files. Initially, all log messages were mixed, making it difficult to track the progress and issues related to specific parts of the pipeline. After implementing specific loggers for each component, the logging system became much clearer and easier to manage.


## Docker File

This project includes a Docker setup to facilitate the deployment and execution of the data pipeline. The Dockerfile defines the environment required to run the application smoothly.

### Requirments

Have install Docker in your computer

### Accessing the Docker Image

To pull the Docker image, use the following command:

```
docker pull saadelbekkali/proyectpipeline
```

### Running the Docker Image

To run the Docker image and execute the data pipeline, use the following command:

```
docker run -it --rm -v $(pwd)/data:/app/data saadelbekkali/proyectpipeline
```
#### Accesing Python Interactively

To access Python interactively within the Docker container, you can run:

```
docker run -it --rm -v $(pwd)/data:/app/data saadelbekkali/proyectpipeline python
```

##### Examples of Queries You Can Launch

Once you have loaded your data into the DuckDB database, you can execute various SQL queries to analyze your data. Here are some examples:


**Connect to the database**
```
import duckdb

conn = duckdb.connect('/app/data/gold/sales_warehouse.db')

```

 ***All Sales Data***

```
df_allsales = conn.execute('SELECT * FROM sales_data LIMIT 10').fetchdf()

print(df_allsales)
```
 
 ***Materialized view***


```
dfMonthly_sales = conn.execute('SELECT * FROM monthly_sales LIMIT 10').fetchdf()

print(dfMonthly_sales)

```
***If you want to exit from Python Interactively***

```
exit()
```