#!/bin/bash

dag_id='ETL_toll_traffic_data_dag'

# dag submission:
cp dag.py /home/project/airflow/dags
airflow dags list | grep "$dag_id" # verify dag is added to airflow dags list
airflow tasks list "$dag_id" # 

echo 'dag submission attempt complete'