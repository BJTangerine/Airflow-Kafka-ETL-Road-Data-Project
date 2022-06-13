# Automated Airflow ETL and Kafka Streaming ETL on Road Traffic Data

Technologies: Python (Apache Airflow/Kafka, pandas, numpy, BeautifulSoup, requests, sqlalchemy) , Bash/shell, PostgreSQL, MySQL.

## Airflow ETL
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

## Kafka ETL
ETL KafkaProducer streaming data into a MySQL database using KafkaConsumer.

Used Python to simulate streaming highway data in a KafkaProducer, and using KafkaConsumer & MySQL to ETL each message into MySQL.

Project files:
* __setup-bash.sh__ - Bash commands for setting up project with Apache Kafka, install Python modules, and MySQL db creation.
* __kafka-zookeeper.sh__ - Starting Kafka Zookeeper.
* __kafka-broker.sh__ - Starting Kafka Broker service.
* __execution-bash.sh__ - Creates Kafka topic in Bash and executes Python KafkaProducer/KafkaConsumer scripts.
* __kafka-producer.py__ - Python script containing streaming data simulator with KafkaProducer.
* __kafka-consumer.py__ - Python script for ETL-ing each message from KafkaProducer into a MySQL db using KafkaConsumer.
* __SimpleLogEngine.py__ - For logging.

## Images
Airflow, transformations engine

![transformer code image preview3](https://user-images.githubusercontent.com/88465305/173453602-db527a11-0e7c-40cc-9cf3-2d6cacaf8caf.PNG)

Airflow, PostgreSQL Table ERD

![Create Table ERD image](https://user-images.githubusercontent.com/88465305/173453394-7b0b7dd2-c8c0-4ffb-a8bc-a598931cd3d5.PNG)

Airflow, SQL View of Total payment Amount by Month/Day pairs if higher than average

![SQL View Total Payment Amount by Month and Day if Higher Than Average Total Payment Amount by Month and Day](https://user-images.githubusercontent.com/88465305/173453513-5fb8b292-aa85-4fa6-8538-8338d119eec4.PNG)

Kafka consumer

![kafka-consumer code snippet image](https://user-images.githubusercontent.com/88465305/173453728-280afd15-6a2a-4a86-bd5c-1645e3f62a03.PNG)
