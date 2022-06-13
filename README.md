# Automated Airflow ETL and Kafka Streaming ETL on Road Traffic Data

Technologies: Python (Apache Airflow, pandas, numpy, BeautifulSoup, requests, sqlalchemy) , Bash/shell, PostgreSQL.

Collect road traffic data from various sources in different formats and combine into one denormalized dataset, in order to upload to an RDBMS for analysis. Currency exchange rates are pulled from an API using requests and BeautifulSoup.

Project Files:
* __dag.py__ - Apache Airflow DAG definitions and pipeline. Utilizes a BashOperator pipeline as all tasks are executed/started using Bash commands.
* __submit-dag-bash.sh__ - Shell script to submit the DAG to Apache Airflow.
* __transform.py__ - Python script which executes the transformations on the consolidated CSV file.
* __TransformationsEngine.py__ - Python script containing the engine used to perform the transformations called in the transform.py script. Here the API called is made if a different currency is specified, to pull the new currency's exchange rate and apply to the payments column.
* __load.py__ - Python script that loads the CSV into PostgreSQL.
* __LoadPostgresqlEngine.py__ - Python script containing the engine for performing the load methods called in the load.py script.
* __road-traffic-project-log.csv__ - Logs written to this CSV by SimpleLogEngine.py.
* __SimpleLogEngine.py__ - Simple user-defined logging function.
* __sql_create_table.sql__ - SQL to create the PostgreSQL table.
* __sql_view.sql__ - SQL to create view of month and day_of_week pairs with total payments above average.
* __sample data files__ - folder containing some of the data sources and files used/created during this ETL process.
